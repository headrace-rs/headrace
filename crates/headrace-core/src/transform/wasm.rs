//! `wasm`: run a user WebAssembly module as a stateless transform (ADR-0018). A `Record`
//! crosses as MessagePack over the module's linear memory; the module returns 0..N `Record`s.
//! The module gets no host imports (empty linker, no WASI); CPU is bounded by an epoch deadline
//! and memory by a store limit. A trap, a limit hit, or undecodable output follows `on_error`
//! (`skip` drops and meters the record, `error` fails the node) - mirroring `map`.

use crate::backend::{Consumer, Producer};
use crate::metrics::{DropReason, NodeMetrics};
use crate::record::Record;
use anyhow::{Context, Result, bail};
use headrace_ir::FaultAction;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use wasmtime::{
    Config, Engine, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder, TypedFunc,
};

/// Linear-memory cap per instance.
const MAX_MEMORY: usize = 64 << 20;
/// The engine epoch is bumped this often; each call gets `DEADLINE_TICKS` of them before a run
/// past its budget traps.
const EPOCH_TICK: Duration = Duration::from_millis(1);
const DEADLINE_TICKS: u64 = 100;

pub(super) struct Spec {
    pub module: String,
    pub sha256: Option<String>,
    pub on_error: FaultAction,
    /// Linear-memory cap (e.g. `64Mi`, `1Gi`); `None` uses the 64 MiB default.
    pub max_memory: Option<String>,
    /// Per-record time budget (e.g. `100ms`, `1s`); `None` uses the 100 ms default.
    pub timeout: Option<String>,
}

impl Spec {
    /// Resolve the string knobs to typed limits, applying defaults and rejecting nonsense.
    fn limits(&self) -> Result<Limits> {
        let mut limits = Limits::default();
        if let Some(s) = &self.max_memory {
            let bytes = parse_size(s)?;
            if bytes == 0 {
                bail!("wasm max_memory must be greater than zero");
            }
            limits.max_memory = bytes;
        }
        if let Some(s) = &self.timeout {
            limits.deadline_ticks = timeout_ticks(s)?;
        }
        Ok(limits)
    }
}

/// Resolved per-node resource limits.
#[derive(Clone, Copy)]
struct Limits {
    max_memory: usize,
    deadline_ticks: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_memory: MAX_MEMORY,
            deadline_ticks: DEADLINE_TICKS,
        }
    }
}

pub(super) async fn run(
    spec: Spec,
    engine: &Engine,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    let limits = spec.limits()?;
    let bytes = std::fs::read(&spec.module)
        .with_context(|| format!("reading wasm module `{}`", spec.module))?;
    if let Some(want) = &spec.sha256 {
        verify_sha256(&bytes, want)?;
    }
    // Compile once; a bad module fails the node up front, like `map` parsing its expression.
    let host = WasmHost::new(engine, &bytes, limits)?;
    let mut inst = host.instantiate()?;
    let mut last_memory = 0;
    while let Some(rec) = rx.recv().await {
        match inst.transform(&rec) {
            Ok(out) => {
                for r in out {
                    if tx.send(r).await.is_err() {
                        return Ok(());
                    }
                    nm.out();
                }
                // Linear memory only grows and then plateaus, so report it when it changes -
                // enough for an operator to right-size `max_memory`.
                let memory = inst.memory_bytes();
                if memory != last_memory {
                    nm.wasm_memory(memory);
                    last_memory = memory;
                }
            }
            Err(e) => {
                if matches!(spec.on_error, FaultAction::Error) {
                    return Err(e).context("wasm transform failed");
                }
                nm.dropped(1, DropReason::Invalid);
                // A trap leaves the store in an undefined state, so start fresh next record.
                inst = host.instantiate()?;
                last_memory = 0;
            }
        }
    }
    Ok(())
}

/// Parse a memory size: a plain byte count, or a number with a binary suffix `Ki`/`Mi`/`Gi`/`Ti`
/// (a trailing `B` is accepted too, so both `64Mi` and `64MiB` work).
fn parse_size(value: &str) -> Result<usize> {
    let v = value.trim();
    let v = v.strip_suffix('B').unwrap_or(v);
    let (num, mult) = if let Some(n) = v.strip_suffix("Ki") {
        (n, 1usize << 10)
    } else if let Some(n) = v.strip_suffix("Mi") {
        (n, 1usize << 20)
    } else if let Some(n) = v.strip_suffix("Gi") {
        (n, 1usize << 30)
    } else if let Some(n) = v.strip_suffix("Ti") {
        (n, 1usize << 40)
    } else {
        (v, 1)
    };
    let n: usize = num
        .trim()
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid memory size `{value}`"))?;
    n.checked_mul(mult)
        .ok_or_else(|| anyhow::anyhow!("memory size `{value}` is too large"))
}

/// Parse a per-record time budget into epoch ticks (one per `EPOCH_TICK`), rounding up.
fn timeout_ticks(value: &str) -> Result<u64> {
    let d = humantime::parse_duration(value)
        .map_err(|e| anyhow::anyhow!("invalid timeout `{value}`: {e}"))?;
    if d.is_zero() {
        bail!("wasm timeout must be greater than zero");
    }
    let ticks = d.as_millis().div_ceil(EPOCH_TICK.as_millis());
    Ok(u64::try_from(ticks).unwrap_or(u64::MAX).max(1))
}

/// Verify a module's SHA-256 against the pinned hex digest.
fn verify_sha256(bytes: &[u8], want: &str) -> Result<()> {
    use sha2::{Digest, Sha256};
    let got = hex_lower(&Sha256::digest(bytes));
    if !got.eq_ignore_ascii_case(want) {
        bail!("wasm module sha256 mismatch: expected {want}, got {got}");
    }
    Ok(())
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Wrap a `wasmtime::Error` with context. wasmtime uses its own error type under
/// `default-features = false`, so anyhow's `.context` does not apply to its `Result`s.
fn wt(e: impl std::fmt::Display, ctx: &str) -> anyhow::Error {
    anyhow::anyhow!("{ctx}: {e}")
}

/// Build the wasm engine and start the one epoch ticker that bounds every module's CPU. Epoch
/// interruption needs the epoch bumped from outside the module, so a background thread does it -
/// one for the whole run, not one per node. Returns the engine plus the ticker's stop flag: set it
/// (see [`WasmEngine`]'s drop) and the thread exits within a tick, so the thread and its engine
/// clone are tied to the engine handle's lifetime rather than leaking for the life of the process.
pub(super) fn build_engine() -> (Engine, Arc<AtomicBool>) {
    let mut config = Config::new();
    config.epoch_interruption(true);
    let engine = Engine::new(&config).expect("valid wasm engine config");
    let stop = Arc::new(AtomicBool::new(false));
    let ticker_engine = engine.clone();
    let ticker_stop = Arc::clone(&stop);
    std::thread::spawn(move || {
        while !ticker_stop.load(Ordering::Relaxed) {
            std::thread::sleep(EPOCH_TICK);
            ticker_engine.increment_epoch();
        }
    });
    (engine, stop)
}

/// A compiled module and its resource limits, ready to instantiate on its engine.
struct WasmHost {
    engine: Engine,
    module: Module,
    limits: Limits,
}

impl WasmHost {
    fn new(engine: &Engine, bytes: &[u8], limits: Limits) -> Result<Self> {
        let module = Module::new(engine, bytes).map_err(|e| wt(e, "compiling the wasm module"))?;
        Ok(Self {
            engine: engine.clone(),
            module,
            limits,
        })
    }

    fn instantiate(&self) -> Result<WasmInstance> {
        let data = StoreData {
            limits: StoreLimitsBuilder::new()
                .memory_size(self.limits.max_memory)
                .build(),
        };
        let mut store = Store::new(&self.engine, data);
        store.limiter(|d| &mut d.limits);
        // A module may run initializers at instantiation, so give that a budget too; `transform`
        // resets a fresh one per record.
        store.set_epoch_deadline(self.limits.deadline_ticks);
        // Empty linker: the module gets no host imports and thus no ambient authority.
        let linker: Linker<StoreData> = Linker::new(&self.engine);
        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| wt(e, "instantiating the wasm module"))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .context("module is missing a `memory` export")?;
        let alloc = instance
            .get_typed_func::<u32, u32>(&mut store, "alloc")
            .map_err(|e| wt(e, "module is missing an `alloc` export"))?;
        let dealloc = instance
            .get_typed_func::<(u32, u32), ()>(&mut store, "dealloc")
            .map_err(|e| wt(e, "module is missing a `dealloc` export"))?;
        let transform = instance
            .get_typed_func::<(u32, u32), u64>(&mut store, "transform")
            .map_err(|e| wt(e, "module is missing a `transform` export"))?;
        // Refuse a module built against an incompatible record shape (ADR-0018). The version
        // bumps only on a breaking change, so an additive change still matches and older modules
        // keep running; a mismatch here means a genuinely incompatible record.
        let abi = instance
            .get_typed_func::<(), u32>(&mut store, "__headrace_abi_version")
            .map_err(|_| {
                anyhow::anyhow!(
                    "module does not declare a Headrace ABI version; rebuild it against the current headrace-wasm-guest"
                )
            })?;
        let got = abi
            .call(&mut store, ())
            .map_err(|e| wt(e, "reading module ABI version"))?;
        if got != headrace_record::ABI_VERSION {
            bail!(
                "wasm module targets ABI v{got}, which is incompatible with this build (ABI v{})",
                headrace_record::ABI_VERSION
            );
        }
        Ok(WasmInstance {
            store,
            memory,
            alloc,
            dealloc,
            transform,
            deadline_ticks: self.limits.deadline_ticks,
            scratch: Vec::new(),
        })
    }
}

struct StoreData {
    limits: StoreLimits,
}

struct WasmInstance {
    store: Store<StoreData>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    dealloc: TypedFunc<(u32, u32), ()>,
    transform: TypedFunc<(u32, u32), u64>,
    deadline_ticks: u64,
    /// Reused input-encoding buffer, to avoid an allocation per record.
    scratch: Vec<u8>,
}

impl WasmInstance {
    /// The module's current linear-memory size in bytes.
    fn memory_bytes(&self) -> u64 {
        self.memory.data_size(&self.store) as u64
    }

    /// Run one record through the module: encode -> alloc+write -> call -> decode in place. A
    /// trap, resource-limit hit, or undecodable output is an `Err`.
    fn transform(&mut self, rec: &Record) -> Result<Vec<Record>> {
        // A fresh CPU budget for this record; a run past it traps at the next epoch check.
        self.store.set_epoch_deadline(self.deadline_ticks);

        // Encode into the reused buffer, then copy into the guest. The bytes must live in the
        // guest's own linear memory, so this one copy in is unavoidable.
        self.scratch.clear();
        rmp_serde::encode::write(&mut self.scratch, rec).context("encoding record")?;
        let in_len = self.scratch.len() as u32;
        let in_ptr = self
            .alloc
            .call(&mut self.store, in_len)
            .map_err(|e| wt(e, "guest alloc"))?;
        self.memory
            .write(&mut self.store, in_ptr as usize, &self.scratch)
            .map_err(|e| wt(e, "writing input to guest memory"))?;

        let packed = self
            .transform
            .call(&mut self.store, (in_ptr, in_len))
            .map_err(|e| wt(e, "wasm transform trapped"))?;
        let _ = self.dealloc.call(&mut self.store, (in_ptr, in_len));

        let out_ptr = (packed >> 32) as usize;
        let out_len = packed as u32 as usize;
        // Decode straight from the guest's memory - no intermediate host buffer or read copy.
        let records = {
            let data = self.memory.data(&self.store);
            let slice = out_ptr
                .checked_add(out_len)
                .and_then(|end| data.get(out_ptr..end))
                .ok_or_else(|| anyhow::anyhow!("wasm output range is out of bounds"))?;
            rmp_serde::from_slice::<Vec<Record>>(slice).context("decoding wasm output")?
        };
        let _ = self
            .dealloc
            .call(&mut self.store, (out_ptr as u32, out_len as u32));
        Ok(records)
    }
}

/// Benchmark-only handle to the transform hot path. Not part of the public API
/// (`#[doc(hidden)]`, semver-exempt); it lets `benches/` drive the real
/// encode -> call -> decode path rather than a copy that could drift from it.
#[doc(hidden)]
pub struct Bench {
    stop: Arc<AtomicBool>,
    inst: WasmInstance,
}

#[doc(hidden)]
impl Bench {
    /// Compile `module` on a fresh engine and instantiate it, ready to transform.
    pub fn new(module: &[u8]) -> Result<Self> {
        let (engine, stop) = build_engine();
        let host = WasmHost::new(&engine, module, Limits::default())?;
        let inst = host.instantiate()?;
        Ok(Self { stop, inst })
    }

    /// Run one record through the module and return the output record count. The instance
    /// (and its engine) are held across calls, so this is the reused-instance steady state.
    pub fn run(&mut self, rec: &Record) -> Result<usize> {
        Ok(self.inst.transform(rec)?.len())
    }
}

impl Drop for Bench {
    fn drop(&mut self) {
        // Stop this handle's epoch ticker, matching `WasmEngine`'s production drop.
        self.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{AttrValue, Attrs};

    // The `headrace-wasm-guest` SDK compiled to wasm32 (examples/wasm; refresh per its README).
    // The real MessagePack ABI end to end: a record in, its value doubled out.
    #[test]
    fn sdk_built_module_doubles_value() {
        let module = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/double.wasm"
        ));
        let (engine, _stop) = build_engine();
        let h = WasmHost::new(&engine, module, Limits::default()).unwrap();
        let mut inst = h.instantiate().unwrap();
        let out = inst.transform(&rec("checkout", 21.0)).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, 42.0);
        assert!(inst.memory_bytes() > 0);
        // Attributes survive the round-trip untouched.
        assert_eq!(
            out[0].attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
    }

    #[test]
    fn bench_handle_runs_the_module() {
        // Covers the benchmark facade (Bench::new/run, drop) on the real SDK module.
        let module = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/double.wasm"
        ));
        let mut b = Bench::new(module).unwrap();
        assert_eq!(b.run(&rec("checkout", 21.0)).unwrap(), 1);
    }

    #[test]
    fn a_trap_is_an_error() {
        let h = host(TRAP);
        let mut inst = h.instantiate().unwrap();
        assert!(inst.transform(&rec("checkout", 1.0)).is_err());
    }

    #[test]
    fn an_infinite_loop_hits_the_epoch_deadline() {
        let h = host(SPIN);
        let mut inst = h.instantiate().unwrap();
        // The background ticker advances the epoch past the deadline and the loop traps.
        assert!(inst.transform(&rec("checkout", 1.0)).is_err());
    }

    #[test]
    fn an_incompatible_abi_version_is_rejected() {
        // BAD_ABI declares a version this build does not support, so instantiation must refuse it.
        let h = host(BAD_ABI);
        assert!(h.instantiate().is_err());
    }

    #[test]
    fn verify_sha256_matches_and_rejects() {
        // sha256("hello")
        let digest = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert!(verify_sha256(b"hello", digest).is_ok());
        assert!(verify_sha256(b"hello", "deadbeef").is_err());
    }

    #[test]
    fn parse_size_reads_binary_units() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("64Mi").unwrap(), 64 << 20);
        assert_eq!(parse_size("1Gi").unwrap(), 1 << 30);
        assert_eq!(parse_size("256MiB").unwrap(), 256 << 20);
        assert!(parse_size("big").is_err());
    }

    #[test]
    fn timeout_ticks_rounds_up_and_rejects_zero() {
        assert_eq!(timeout_ticks("100ms").unwrap(), 100);
        assert_eq!(timeout_ticks("1s").unwrap(), 1000);
        assert_eq!(timeout_ticks("500us").unwrap(), 1); // sub-tick rounds up to one tick
        assert!(timeout_ticks("0ms").is_err());
        assert!(timeout_ticks("nope").is_err());
    }

    // Helpers and fixtures. The trap and epoch cases need modules with specific low-level behavior,
    // so they are tiny modules hand-written in WAT (WebAssembly's text form); the ABI round-trip
    // above uses the real SDK-built module instead.
    const TRAP: &str = r#"(module
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 16))
      (func (export "dealloc") (param i32) (param i32))
      (func (export "__headrace_abi_version") (result i32) (i32.const 1))
      (func (export "transform") (param i32) (param i32) (result i64) (unreachable)))"#;

    const SPIN: &str = r#"(module
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 16))
      (func (export "dealloc") (param i32) (param i32))
      (func (export "__headrace_abi_version") (result i32) (i32.const 1))
      (func (export "transform") (param i32) (param i32) (result i64)
        (loop $l (br $l)) (i64.const 0)))"#;

    const BAD_ABI: &str = r#"(module
      (memory (export "memory") 1)
      (func (export "alloc") (param i32) (result i32) (i32.const 16))
      (func (export "dealloc") (param i32) (param i32))
      (func (export "__headrace_abi_version") (result i32) (i32.const 999))
      (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#;

    fn host(wat: &str) -> WasmHost {
        // Tests drive the engine directly, so nothing sets the ticker's stop flag; the thread just
        // runs for the (short-lived) test process. Production stops it when `WasmEngine` drops.
        let (engine, _stop) = build_engine();
        WasmHost::new(&engine, &wat::parse_str(wat).unwrap(), Limits::default()).unwrap()
    }

    fn rec(svc: &str, value: f64) -> Record {
        let mut attrs = Attrs::new();
        attrs.insert("service.name".into(), AttrValue::Str(svc.into()));
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: "m".into(),
            value,
            attrs,
        }
    }
}
