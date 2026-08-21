---
title: wasm
description: Run a sandboxed WebAssembly module as a custom per-record transform.
showAskAi: false
---

# Wasm

The `wasm` transform runs a **WebAssembly module** as a stateless, per-record transform: one
`Record` in, zero or more out (transform, drop, or fan-out). It is the escape hatch for logic the
fixed catalog (`filter`, `map`, `window`, `join`) can't express. Headrace hands the module each
record as MessagePack bytes and reads the results back the same way.

```yaml
transforms:
  - type: wasm
    id: score
    input: rolled_up
    module: ./modules/score.wasm    # a path, file://, or oci://...@sha256: (see Sourcing)
    sha256: "9f86d0..."             # optional: pin the module's digest
    max_memory: 128Mi               # optional: linear-memory cap (default 64Mi)
    timeout: 200ms                  # optional: time budget per record (default 100ms)
    on_error: skip                  # crash / bad output: skip | error (default skip)
```

An empty output drops the record; several fan it out. The module is loaded and compiled once when
the node starts, so a missing file or a `sha256` mismatch fails the pipeline immediately.

## Authoring in Rust

The `headrace-wasm-guest` crate turns an `fn(Record) -> Vec<Record>` into a module. Annotate your
function with `#[transform]` and build a `cdylib` for `wasm32-unknown-unknown`:

```rust
use headrace_wasm_guest::{transform, Record};

#[transform]
fn double(mut rec: Record) -> Vec<Record> {
    rec.value *= 2.0;
    vec![rec]
}
```

```sh
cargo build --release --target wasm32-unknown-unknown
# point `module:` at target/wasm32-unknown-unknown/release/<crate>.wasm
```

Your function receives an **owned** `Record`, so it is free to modify it, drop it (return an empty
`Vec`), or emit several. It is ordinary safe Rust - the unsafe code that moves bytes across the
boundary lives in the SDK, not in your module. The
[`examples/wasm`](https://github.com/headrace-rs/headrace/tree/main/examples/wasm) crate is a
complete, buildable module.

## Other languages

The boundary is a language-neutral **bytes ABI**, so any language that compiles to core wasm and
can read and write MessagePack can author a module - Go (via TinyGo), C, AssemblyScript. Only Rust
has a first-class SDK today; other languages implement the ABI by hand:

- export a `memory`, plus `alloc(len) -> ptr` and `dealloc(ptr, len)` (Headrace calls `alloc` to get
  a place to write the input into the module's memory);
- export `transform(ptr, len) -> i64`: decode the MessagePack `Record` in `[ptr, ptr + len)`, then
  return the output `Vec<Record>` encoded as MessagePack, packing its `(ptr, len)` into the result
  (`ptr << 32 | len`);
- export `__headrace_abi_version() -> i32` returning the ABI version the module targets (currently
  `1`). This version bumps only on a *breaking* change to the record, so an additive change (a new
  field) keeps the same version and older modules keep running; Headrace refuses a module only when
  the versions genuinely differ.

The contract to honor: `alloc(len)` returns a pointer to `len` writable bytes that stay valid until
`transform` reads them, and `transform` returns a pointer to bytes that stay valid until Headrace
reads them (do not free the output before returning). Get this wrong and the sandbox still contains
it - a bad pointer or length is caught by wasm's bounds checks and surfaces through `on_error`, never
as host corruption; the worst case is a dropped or malformed record.

## Sourcing

The `module` field is a URI (ADR-0019):

- a **local path** or `file://` URI, read once when the node starts, with an optional `sha256` pin;
- an `oci://<registry>/<repository>@sha256:<digest>` reference, pulled from a registry at startup.

An `oci://` reference must be **digest-pinned**; a mutable tag is refused, because the code a node
runs must not change under it. The digest is the integrity check, so `sha256` is redundant for
`oci://` (it stays optional for files).

```yaml
module: oci://ghcr.io/acme/score@sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08
```

Registry pulls need the **`wasm-oci` build** (`cargo build --features wasm-oci`); the default binary
stays lean and file-only. The module is pulled once, verified against the digest, and cached on disk
content-addressed, so a restart or a co-located node does not refetch.

Two operator controls, both **CLI-only**:

- `--wasm-allow-registry <host>` (repeatable) allowlists the registries a pull may use. It is
  **empty by default, so every `oci://` pull is denied** until you permit its registry. It is never
  read from the pipeline file: the pipeline author must not be able to widen which registries the
  process fetches and runs code from.
- `--wasm-cache-dir <dir>` sets the cache location (default: a per-OS temp directory).

Pulls authenticate from the ambient Docker or podman credential chain (`docker login` or
`podman login`), falling back to anonymous for a public registry.

With Helm, set `module: oci://...@sha256:...` inline in the pipeline config and pass
`--wasm-allow-registry` on the container args - no image rebuild and no ConfigMap-mounted binary.

## Sandbox

A module is pure computation with no access to the outside world:

- **no host imports, no WASI** - it cannot touch the filesystem, network, clock, or environment
  variables; all it can do is turn input records into output records;
- **memory capped** per module instance (`max_memory`, default 64 MiB); the
  `headrace.wasm.memory.bytes` metric reports actual usage so you can size it from real data;
- **a time budget per record** (`timeout`, default 100 ms) - each `transform` call gets a fresh
  budget; one that runs too long (say an accidental infinite loop) is stopped rather than hanging
  the worker.

If a module stops abnormally - it crashes (a *trap*), exceeds its time or memory budget, or returns
output Headrace can't decode - **`on_error`** decides what happens, exactly like `map`'s
`on_invalid`: `skip` drops that one record and counts it on `headrace.records.dropped`
(`reason=invalid`); `error` stops the pipeline. Both default to `skip`.

## Performance

A `wasm` transform runs in **microseconds per record**, sub-millisecond even for wide records, on a
reused instance (compiled once at startup). Representative figures for the `examples/wasm` module
(doubles a value), on an Apple M4, single thread (`us` = microsecond):

| Record (attributes) | Latency per record | Throughput, one core |
| ------------------- | ------------------ | -------------------- |
| 1                   | ~1.7 us            | ~600K rec/s          |
| 10                  | ~8.9 us            | ~112K rec/s          |
| 50                  | ~46 us             | ~22K rec/s           |

Latency tracks a record's attribute count because the cost is the MessagePack round-trip: encode
into the module, decode inside it, encode the result, decode it back out. The host-side encode alone
is 27 ns to 0.6 us across that same range - a small fraction of each figure - so the rest is the
guest's own decode/encode and the call boundary, not Headrace's marshalling. A zero-copy archived
format (rkyv) could trim the round-trip but would change the guest API; the current numbers clear
sub-millisecond by a wide margin, so it stays deferred (ADR-0018).

Treat these as orders of magnitude, not guarantees: they move with hardware, module complexity, and
record shape. Reproduce them on your own machine with:

```sh
cargo bench -p headrace-core --features wasm --bench wasm_transform
```
