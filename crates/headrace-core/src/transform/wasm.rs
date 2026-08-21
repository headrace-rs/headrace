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
    oci: &OciSource,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    let limits = spec.limits()?;
    let bytes = load_module(&ModuleRef::parse(&spec.module)?, oci).await?;
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

/// A parsed `module` reference (ADR-0019): a local file, or a digest-pinned OCI artifact.
/// `file://` (RFC 8089) and a bare path both resolve to a file; `oci://` to a registry pull.
enum ModuleRef {
    File(std::path::PathBuf),
    Oci(OciRef),
}

/// An `oci://<registry>/<repository>@sha256:<digest>` reference. Always digest-pinned: the digest
/// is the integrity check, and a mutable tag is refused because it can change under a running node.
struct OciRef {
    /// The reference without the `oci://` scheme: `<registry>/<repository>@sha256:<digest>`.
    reference: String,
}

impl ModuleRef {
    fn parse(module: &str) -> Result<Self> {
        if let Some(rest) = module.strip_prefix("oci://") {
            return OciRef::parse(rest).map(ModuleRef::Oci);
        }
        if let Some(rest) = module.strip_prefix("file://") {
            return Ok(ModuleRef::File(file_url_path(rest)));
        }
        // Bare path shorthand, so configs written before ADR-0019 keep working.
        Ok(ModuleRef::File(std::path::PathBuf::from(module)))
    }
}

impl OciRef {
    fn parse(rest: &str) -> Result<Self> {
        let (name, digest) = rest.rsplit_once('@').ok_or_else(|| {
            anyhow::anyhow!(
                "oci module `{rest}` must be digest-pinned as `<registry>/<repository>@sha256:<digest>`; a mutable tag is refused"
            )
        })?;
        let hex = digest.strip_prefix("sha256:").ok_or_else(|| {
            anyhow::anyhow!("oci module digest must be `sha256:<hex>`, got `{digest}`")
        })?;
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            bail!("oci module digest `{digest}` is not a 64-character sha256");
        }
        if name.starts_with('/') || !name.contains('/') {
            bail!("oci module `{rest}` needs a `<registry>/<repository>` name");
        }
        Ok(Self {
            reference: rest.to_string(),
        })
    }
}

/// Resolve a `file://` body to a path (RFC 8089): an empty or `localhost` authority is the local
/// host, so `file:///p` and `file://localhost/p` both mean `/p`.
fn file_url_path(rest: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(rest.strip_prefix("localhost").unwrap_or(rest))
}

/// Where `oci://` modules may be pulled from and cached (ADR-0019). Operator-set from the CLI and
/// carried on the shared engine handle; a pipeline file cannot set it, because the allowed
/// registries are a trust boundary the pipeline author must not be able to widen. The fields are
/// present only with the `wasm-oci` feature.
#[derive(Clone, Default)]
pub(crate) struct OciSource {
    #[cfg(feature = "wasm-oci")]
    allow: Arc<[String]>,
    #[cfg(feature = "wasm-oci")]
    cache_dir: std::path::PathBuf,
}

impl OciSource {
    /// Build from the CLI allowlist and cache dir; the cache defaults to a per-OS temp
    /// subdirectory when unset. Without `wasm-oci` the inputs are unused.
    pub(crate) fn new(allow: Vec<String>, cache_dir: Option<std::path::PathBuf>) -> Self {
        #[cfg(feature = "wasm-oci")]
        {
            Self {
                allow: allow.into(),
                cache_dir: cache_dir.unwrap_or_else(|| std::env::temp_dir().join("headrace-wasm")),
            }
        }
        #[cfg(not(feature = "wasm-oci"))]
        {
            let _ = (allow, cache_dir);
            Self {}
        }
    }
}

/// Load a module's bytes. A file is read at node start (once); an `oci://` reference is pulled and
/// cached by the `wasm-oci` path, or rejected with guidance when that feature is off.
async fn load_module(module: &ModuleRef, oci: &OciSource) -> Result<Vec<u8>> {
    match module {
        ModuleRef::File(path) => {
            std::fs::read(path).with_context(|| format!("reading wasm module `{}`", path.display()))
        }
        ModuleRef::Oci(r) => pull_oci(r, oci).await,
    }
}

/// Without the feature, an `oci://` reference cannot be fetched, so fail the node with guidance
/// rather than silently.
#[cfg(not(feature = "wasm-oci"))]
async fn pull_oci(r: &OciRef, _oci: &OciSource) -> Result<Vec<u8>> {
    bail!(
        "oci module `{}` needs the `wasm-oci` feature; rebuild with `--features wasm-oci`",
        r.reference
    )
}

/// Pull an `oci://` module: check the registry against the allowlist, serve a cache hit if we
/// have one, else fetch and cache. The digest pin makes the fetch verify content and makes a
/// cache hit unambiguous.
#[cfg(feature = "wasm-oci")]
async fn pull_oci(r: &OciRef, oci: &OciSource) -> Result<Vec<u8>> {
    let reference: oci_client::Reference = r
        .reference
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid oci reference `{}`: {e}", r.reference))?;
    let registry = reference.resolve_registry();

    // The allowlist is an operator trust boundary (CLI only). Empty denies every pull.
    if !oci.allow.iter().any(|a| a == registry) {
        bail!(
            "oci registry `{registry}` is not allowed; permit it with `--wasm-allow-registry {registry}`"
        );
    }

    let digest = reference.digest().unwrap_or_default();
    if let Some(bytes) = cache_load(oci, digest).await {
        return Ok(bytes);
    }
    let bytes = fetch_oci(&reference, registry).await?;
    cache_store(oci, digest, &bytes).await;
    Ok(bytes)
}

/// Read the content-addressed cache for `digest` (`None` on a miss), so a restart or a co-located
/// node reuses the artifact instead of refetching.
#[cfg(feature = "wasm-oci")]
async fn cache_load(oci: &OciSource, digest: &str) -> Option<Vec<u8>> {
    tokio::fs::read(cache_path(oci, digest)).await.ok()
}

/// Write the module to the cache, best effort: a failure just means the next node refetches.
#[cfg(feature = "wasm-oci")]
async fn cache_store(oci: &OciSource, digest: &str, bytes: &[u8]) {
    let path = cache_path(oci, digest);
    if let Some(dir) = path.parent() {
        let _ = tokio::fs::create_dir_all(dir).await;
    }
    let _ = tokio::fs::write(path, bytes).await;
}

#[cfg(feature = "wasm-oci")]
fn cache_path(oci: &OciSource, digest: &str) -> std::path::PathBuf {
    let key = digest.strip_prefix("sha256:").unwrap_or(digest);
    oci.cache_dir.join(format!("{key}.wasm"))
}

/// Fetch the module layer from the registry, authenticating from the ambient credential chain.
#[cfg(feature = "wasm-oci")]
async fn fetch_oci(reference: &oci_client::Reference, registry: &str) -> Result<Vec<u8>> {
    let client =
        oci_wasm::WasmClient::new(oci_client::Client::new(oci_client::client::ClientConfig {
            protocol: oci_protocol(registry),
            ..Default::default()
        }));
    let image = client
        .pull(reference, &registry_auth(registry))
        .await
        .with_context(|| format!("pulling oci module `{reference}`"))?;
    image
        .layers
        .into_iter()
        .next()
        .map(|l| l.data.to_vec())
        .ok_or_else(|| anyhow::anyhow!("oci module `{reference}` has no layers"))
}

/// HTTP for a loopback registry (a co-located or sidecar registry, not a MITM surface), HTTPS for
/// everything else - so a local registry needs no TLS while remote pulls stay encrypted.
#[cfg(feature = "wasm-oci")]
fn oci_protocol(registry: &str) -> oci_client::client::ClientProtocol {
    use oci_client::client::ClientProtocol;
    if is_loopback(registry) {
        ClientProtocol::HttpsExcept(vec![registry.to_string()])
    } else {
        ClientProtocol::Https
    }
}

/// Whether a registry host is loopback (localhost / 127.0.0.1 / ::1), ignoring any `:port`.
#[cfg(feature = "wasm-oci")]
fn is_loopback(registry: &str) -> bool {
    let host = registry.rsplit_once(':').map_or(registry, |(h, _)| h);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host == "localhost" || host == "127.0.0.1" || host == "::1"
}

/// Resolve registry credentials, falling back to anonymous. Tries the Docker credential chain
/// (config.json, credential helpers) first, then podman's auth file, so both `docker login` and
/// `podman login` work.
#[cfg(feature = "wasm-oci")]
fn registry_auth(registry: &str) -> oci_client::secrets::RegistryAuth {
    use oci_client::secrets::RegistryAuth;
    // An identity token is not basic auth, so only UsernamePassword maps here.
    if let Ok(docker_credential::DockerCredential::UsernamePassword(user, pass)) =
        docker_credential::get_credential(registry)
    {
        return RegistryAuth::Basic(user, pass);
    }
    if let Some((user, pass)) = podman_credential(registry) {
        return RegistryAuth::Basic(user, pass);
    }
    RegistryAuth::Anonymous
}

/// Look up Basic credentials in podman's auth file. Its format matches docker's config.json (an
/// `auths` map with a base64 `user:pass` per registry), so `podman login` is supported the same
/// way as `docker login`; only the file location differs.
#[cfg(feature = "wasm-oci")]
fn podman_credential(registry: &str) -> Option<(String, String)> {
    podman_credential_at(&podman_auth_path()?, registry)
}

#[cfg(feature = "wasm-oci")]
fn podman_credential_at(path: &std::path::Path, registry: &str) -> Option<(String, String)> {
    use base64::Engine;
    let json: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    let auth = json.get("auths")?.get(registry)?.get("auth")?.as_str()?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(auth)
        .ok()?;
    String::from_utf8(decoded)
        .ok()?
        .split_once(':')
        .map(|(u, p)| (u.to_string(), p.to_string()))
}

/// Podman's auth file: `REGISTRY_AUTH_FILE`, then the rootless runtime dir, then the XDG config dir.
#[cfg(feature = "wasm-oci")]
fn podman_auth_path() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("REGISTRY_AUTH_FILE") {
        return Some(p.into());
    }
    if let Ok(rt) = std::env::var("XDG_RUNTIME_DIR") {
        let p = std::path::Path::new(&rt).join("containers/auth.json");
        if p.exists() {
            return Some(p);
        }
    }
    let config = std::env::var("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .ok()
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| std::path::Path::new(&h).join(".config"))
        })?;
    let p = config.join("containers/auth.json");
    p.exists().then_some(p)
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

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn run_transforms_and_forwards_output() {
        let module = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/double.wasm");
        let (res, out) = drive_wasm(module, FaultAction::Skip, rec("checkout", 21.0)).await;
        res.unwrap();
        assert_eq!(out, vec![42.0], "the module doubles and forwards the value");
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn run_skips_a_trap_and_keeps_going() {
        let module = write_temp_module("trap-skip", TRAP);
        let (res, out) = drive_wasm(&module, FaultAction::Skip, rec("checkout", 1.0)).await;
        res.unwrap(); // skip drops the record; the node runs on to close
        assert!(out.is_empty(), "a trapped record is not forwarded");
    }

    #[cfg(feature = "mocks")]
    #[tokio::test]
    async fn run_fails_a_trap_under_on_error_error() {
        let module = write_temp_module("trap-error", TRAP);
        let (res, _) = drive_wasm(&module, FaultAction::Error, rec("checkout", 1.0)).await;
        assert!(res.is_err(), "on_error=error fails the node");
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

    const DIGEST: &str = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    #[test]
    fn module_ref_parses_file_and_bare_paths() {
        use std::path::Path;
        assert!(matches!(
            ModuleRef::parse("./m.wasm").unwrap(),
            ModuleRef::File(p) if p == Path::new("./m.wasm")
        ));
        assert!(matches!(
            ModuleRef::parse("file:///abs/m.wasm").unwrap(),
            ModuleRef::File(p) if p == Path::new("/abs/m.wasm")
        ));
        assert!(matches!(
            ModuleRef::parse("file://localhost/abs/m.wasm").unwrap(),
            ModuleRef::File(p) if p == Path::new("/abs/m.wasm")
        ));
    }

    #[test]
    fn module_ref_parses_a_digest_pinned_oci_reference() {
        let m = ModuleRef::parse(&format!("oci://ghcr.io/org/mod@{DIGEST}")).unwrap();
        assert!(
            matches!(m, ModuleRef::Oci(o) if o.reference == format!("ghcr.io/org/mod@{DIGEST}"))
        );
    }

    #[test]
    fn module_ref_rejects_a_mutable_or_malformed_oci_reference() {
        assert!(ModuleRef::parse("oci://ghcr.io/org/mod:v1").is_err()); // a tag, not a digest
        assert!(ModuleRef::parse(&format!("oci://mod@{DIGEST}")).is_err()); // no registry/repo split
        assert!(ModuleRef::parse("oci://ghcr.io/org/mod@md5:abcd").is_err()); // not sha256
        assert!(ModuleRef::parse("oci://ghcr.io/org/mod@sha256:dead").is_err()); // wrong length
    }

    #[tokio::test]
    async fn load_module_reads_a_file_path() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/double.wasm");
        let bytes = load_module(&ModuleRef::parse(path).unwrap(), &OciSource::default())
            .await
            .unwrap();
        assert!(!bytes.is_empty());
    }

    #[cfg(not(feature = "wasm-oci"))]
    #[tokio::test]
    async fn load_module_rejects_oci_without_the_feature() {
        let m = ModuleRef::parse(&format!("oci://ghcr.io/org/mod@{DIGEST}")).unwrap();
        assert!(load_module(&m, &OciSource::default()).await.is_err());
    }

    #[cfg(feature = "wasm-oci")]
    #[tokio::test]
    async fn oci_pull_rejects_a_registry_off_the_allowlist() {
        let m = ModuleRef::parse(&format!("oci://ghcr.io/org/mod@{DIGEST}")).unwrap();
        // An empty allowlist denies all, so this returns before touching the network.
        let err = load_module(&m, &OciSource::new(vec![], None))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("not allowed"), "unexpected error: {err}");
    }

    #[cfg(feature = "wasm-oci")]
    #[tokio::test]
    async fn oci_pull_serves_a_cache_hit_without_the_network() {
        let dir = std::env::temp_dir().join("headrace-oci-hit-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let key = DIGEST.strip_prefix("sha256:").unwrap();
        tokio::fs::write(dir.join(format!("{key}.wasm")), b"cached")
            .await
            .unwrap();
        let m = ModuleRef::parse(&format!("oci://ghcr.io/org/mod@{DIGEST}")).unwrap();
        let oci = OciSource::new(vec!["ghcr.io".to_string()], Some(dir.clone()));
        assert_eq!(load_module(&m, &oci).await.unwrap(), b"cached");
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[cfg(feature = "wasm-oci")]
    #[tokio::test]
    async fn oci_cache_load_and_store_round_trip() {
        let dir = std::env::temp_dir().join("headrace-oci-roundtrip-test");
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let oci = OciSource::new(vec![], Some(dir.clone()));
        assert!(cache_load(&oci, DIGEST).await.is_none()); // miss
        cache_store(&oci, DIGEST, b"abc").await;
        assert_eq!(cache_load(&oci, DIGEST).await.unwrap(), b"abc"); // hit
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[cfg(feature = "wasm-oci")]
    #[test]
    fn registry_auth_falls_back_to_anonymous() {
        use oci_client::secrets::RegistryAuth;
        // No credential is configured for this host in CI, so anonymous (basic if a dev has one).
        assert!(matches!(
            registry_auth("registry.invalid.example"),
            RegistryAuth::Anonymous | RegistryAuth::Basic(..)
        ));
    }

    #[cfg(feature = "wasm-oci")]
    #[test]
    fn podman_credential_reads_the_auth_file() {
        use base64::Engine;
        let dir = std::env::temp_dir().join("headrace-podman-auth-test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.json");
        let auth = base64::engine::general_purpose::STANDARD.encode("user:pass");
        std::fs::write(
            &path,
            format!(r#"{{"auths":{{"ghcr.io":{{"auth":"{auth}"}}}}}}"#),
        )
        .unwrap();
        assert_eq!(
            podman_credential_at(&path, "ghcr.io"),
            Some(("user".to_string(), "pass".to_string()))
        );
        assert_eq!(podman_credential_at(&path, "other.io"), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(feature = "wasm-oci")]
    #[test]
    fn is_loopback_ignores_the_port() {
        assert!(is_loopback("127.0.0.1:5000"));
        assert!(is_loopback("localhost:49223"));
        assert!(is_loopback("[::1]:5000"));
        assert!(is_loopback("localhost"));
        assert!(!is_loopback("ghcr.io"));
        assert!(!is_loopback("ghcr.io:443"));
    }

    #[cfg(feature = "wasm-oci")]
    #[test]
    fn oci_protocol_uses_http_only_for_loopback() {
        use oci_client::client::ClientProtocol;
        assert!(matches!(
            oci_protocol("127.0.0.1:5000"),
            ClientProtocol::HttpsExcept(_)
        ));
        assert!(matches!(oci_protocol("ghcr.io"), ClientProtocol::Https));
    }

    // End-to-end over a real registry (testcontainers): push the SDK fixture as an OCI wasm
    // artifact, then pull it back through `load_module` -> `fetch_oci`, covering the network path
    // the other tests cannot. Not `#[ignore]`, so CI's coverage job (Docker present) runs it; it
    // self-skips when no container runtime is available, so a plain `cargo test` still passes.
    #[cfg(feature = "wasm-oci")]
    #[tokio::test]
    async fn oci_push_and_pull_round_trip_over_a_registry() {
        use testcontainers::GenericImage;
        use testcontainers::core::{IntoContainerPort, WaitFor};
        use testcontainers::runners::AsyncRunner;

        let started = GenericImage::new("registry", "2")
            .with_wait_for(WaitFor::message_on_stderr("listening on"))
            .with_exposed_port(5000.tcp())
            .start()
            .await;
        let container = match started {
            Ok(c) => c,
            Err(e) => {
                eprintln!("skipping oci e2e: no container runtime ({e})");
                return;
            }
        };
        let port = container.get_host_port_ipv4(5000.tcp()).await.unwrap();
        let registry = format!("127.0.0.1:{port}");

        let wasm = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/double.wasm"
        ))
        .unwrap();

        // Push the module under a tag; the loopback registry speaks plain HTTP.
        let tag: oci_client::Reference = format!("{registry}/headrace/double:latest")
            .parse()
            .unwrap();
        let (config, layer) = oci_wasm::WasmConfig::from_raw_module(wasm.clone(), None).unwrap();
        let push_client =
            oci_wasm::WasmClient::new(oci_client::Client::new(oci_client::client::ClientConfig {
                protocol: oci_client::client::ClientProtocol::HttpsExcept(vec![registry.clone()]),
                ..Default::default()
            }));
        push_client
            .push(
                &tag,
                &oci_client::secrets::RegistryAuth::Anonymous,
                layer,
                config,
                None,
            )
            .await
            .expect("push the wasm artifact");
        // Pin the pull to the manifest digest, read from the registry rather than guessed.
        let base = oci_client::Client::new(oci_client::client::ClientConfig {
            protocol: oci_client::client::ClientProtocol::HttpsExcept(vec![registry.clone()]),
            ..Default::default()
        });
        let digest = base
            .fetch_manifest_digest(&tag, &oci_client::secrets::RegistryAuth::Anonymous)
            .await
            .expect("fetch the manifest digest");
        assert!(digest.starts_with("sha256:"), "digest: {digest}");

        let dir = std::env::temp_dir().join(format!("headrace-oci-e2e-{port}"));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        let m = ModuleRef::parse(&format!("oci://{registry}/headrace/double@{digest}")).unwrap();
        let oci = OciSource::new(vec![registry.clone()], Some(dir.clone()));

        // First load pulls over the network (fetch_oci); the bytes match what we pushed.
        let pulled = load_module(&m, &oci).await.expect("pull the wasm module");
        assert_eq!(pulled, wasm, "pulled module matches the pushed bytes");
        // The pull populated the content-addressed cache, and a second load is served from it.
        assert!(
            cache_load(&oci, &digest).await.is_some(),
            "module is cached"
        );
        assert_eq!(load_module(&m, &oci).await.unwrap(), wasm);
        let _ = tokio::fs::remove_dir_all(&dir).await;
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

    // Drive one record through the node loop `run` with mock edges, returning the run result and
    // the values it forwarded. Exercises `run` and, via the spec, `Spec::limits`.
    #[cfg(feature = "mocks")]
    async fn drive_wasm(
        module: &str,
        on_error: FaultAction,
        input: Record,
    ) -> (Result<()>, Vec<f64>) {
        use crate::backend::{MockConsumer, MockProducer};
        use crate::metrics::{NodeKind, NoopMetrics, SharedMetrics};
        use std::sync::{Arc, Mutex};

        // One record, then the edge closes.
        let pending = Mutex::new(Some(input));
        let mut rx = MockConsumer::new();
        rx.expect_recv()
            .times(0..)
            .returning(move || pending.lock().unwrap().take());

        let out = Arc::new(Mutex::new(Vec::new()));
        let sink = out.clone();
        let mut tx = MockProducer::new();
        tx.expect_send().times(0..).returning(move |rec| {
            sink.lock().unwrap().push(rec.value);
            Ok(())
        });

        let m: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&m, "w", NodeKind::Wasm);
        // Some(...) knobs exercise the resolve paths in Spec::limits.
        let spec = Spec {
            module: module.to_string(),
            sha256: None,
            on_error,
            max_memory: Some("128Mi".into()),
            timeout: Some("200ms".into()),
        };
        let (engine, _stop) = build_engine();
        let res = run(
            spec,
            &engine,
            &OciSource::default(),
            Box::new(rx),
            Box::new(tx),
            nm,
        )
        .await;
        let out = Arc::try_unwrap(out).unwrap().into_inner().unwrap();
        (res, out)
    }

    // Compile `wat` to a temp `.wasm` file so `run` can source it by path.
    #[cfg(feature = "mocks")]
    fn write_temp_module(tag: &str, wat: &str) -> String {
        let dir = std::env::temp_dir().join(format!("headrace-wasm-run-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.wasm");
        std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
        path.to_string_lossy().into_owned()
    }
}
