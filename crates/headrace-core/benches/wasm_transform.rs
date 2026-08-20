//! Micro-benchmarks for the `wasm` transform (ADR-0018): per-record latency and throughput of
//! the encode -> alloc+write -> call -> decode-in-place path, on a reused instance. Run with:
//!
//! ```sh
//! cargo bench -p headrace-core --features wasm --bench wasm_transform
//! ```
//!
//! Two groups: `wasm/transform` is the whole path end to end (the headline number), and
//! `wasm/encode_only` is just the host-side MessagePack encode, so the gap between them is the
//! guest call plus decode. Both scale the record by attribute count, since the payload is what
//! the marshalling copies. Without `--features wasm` this compiles to an empty binary.

#[cfg(feature = "wasm")]
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
#[cfg(feature = "wasm")]
use headrace_core::record::{AttrValue, Attrs, Record};
#[cfg(feature = "wasm")]
use headrace_core::transform::WasmBench;

// The SDK-built example module: doubles `value`, passes attributes through unchanged. Same
// fixture the host test loads, so the bench measures a real module, not a hand-written stub.
#[cfg(feature = "wasm")]
const DOUBLE: &[u8] = include_bytes!("../tests/fixtures/double.wasm");

// Record sizes by attribute count: a bare record, a typical one, and a wide one.
#[cfg(feature = "wasm")]
const SIZES: &[usize] = &[1, 10, 50];

#[cfg(feature = "wasm")]
fn transform_latency(c: &mut Criterion) {
    let mut g = c.benchmark_group("wasm/transform");
    for &n in SIZES {
        let r = record(n);
        let mut h = WasmBench::new(DOUBLE).expect("build wasm bench harness");
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::from_parameter(n), &r, move |b, r| {
            b.iter(|| black_box(h.run(black_box(r)).unwrap()));
        });
    }
    g.finish();
}

#[cfg(feature = "wasm")]
fn encode_only(c: &mut Criterion) {
    let mut g = c.benchmark_group("wasm/encode_only");
    for &n in SIZES {
        let r = record(n);
        let mut buf = Vec::new();
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(BenchmarkId::from_parameter(n), &r, move |b, r| {
            b.iter(|| {
                buf.clear();
                rmp_serde::encode::write(&mut buf, black_box(r)).unwrap();
                black_box(buf.len())
            });
        });
    }
    g.finish();
}

// A record with `n` attributes (one is `service.name`, the rest are filler), to vary payload size.
#[cfg(feature = "wasm")]
fn record(n: usize) -> Record {
    let mut attrs = Attrs::new();
    attrs.insert("service.name".into(), AttrValue::Str("checkout".into()));
    for i in 0..n.saturating_sub(1) {
        attrs.insert(format!("attr.{i}"), AttrValue::Str(format!("value-{i}")));
    }
    Record {
        ts_nanos: 1,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "http.server.duration".into(),
        value: 21.0,
        attrs,
    }
}

#[cfg(feature = "wasm")]
criterion_group!(benches, transform_latency, encode_only);
#[cfg(feature = "wasm")]
criterion_main!(benches);

#[cfg(not(feature = "wasm"))]
fn main() {}
