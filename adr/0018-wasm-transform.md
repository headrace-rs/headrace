# 0018. WASM transform: a bytes ABI, sandboxed, local modules

- Status: Accepted
- Date: 2026-08-17

## Context

Headrace has a fixed transform catalog (filter, window, map, join). The `wasm` transform is the
escape hatch for custom per-record logic. First scope is **stateless, map-style**: one `Record` in,
0..N `Record`s out (transform, drop, or fan-out). Stateful WASM is out of scope. This pins the
runtime, the host/guest interface, the sandbox, and how a module is sourced.

## Decision

- **Runtime: wasmtime** (`default-features = false`, features `runtime` + `cranelift`). Mature, and
  it gives us the sandbox controls we need out of the box - a way to bound a module's CPU time and
  its memory; it is what Redpanda and Fluvio run their per-record transforms on.
- **Interface: a core-wasm bytes ABI, not the Component Model (yet).** The host serializes a
  `Record` to MessagePack (the codec already on the NATS wire) and the guest returns MessagePack
  `Vec<Record>`. Guest exports: `memory`, `alloc(len) -> ptr`, `dealloc(ptr, len)`, and
  `transform(ptr, len) -> i64` whose result packs the output `(ptr, len)`. An empty vec is a drop;
  several records fan out. This keeps the host trivial and reuses our codec. The Component Model
  (WIT + wit-bindgen) is a later upgrade for typed cross-language bindings; we hold the logical
  `Record -> list<Record>` interface stable so that switch is a host+SDK swap, not a behavior change.
- **Authoring: a Rust SDK first.** A `#[transform]` attribute (from `headrace-wasm-guest`) emits the
  ABI exports so an author writes `fn(Record) -> Vec<Record>`. The ABI is language-neutral, so other
  languages that target wasm and can MessagePack work too (Go via TinyGo); they implement the ABI by
  hand until per-language SDKs or the Component Model arrive.
- **Sandbox: no access to the outside world.** The module gets no host functions and no WASI (an
  empty linker), so it cannot touch the filesystem, network, or clock. Its CPU is bounded by a
  per-record time budget (wasmtime's epoch interruption: a background thread ticks a counter, each
  call gets a deadline in ticks, and a run past it is stopped); its memory by a `StoreLimits` cap.
- **Sourcing: a local file path** with an optional `sha256`. Fetching a module from a registry or
  URL stays out of the transform: the deploy layer mounts the `.wasm` as it mounts the pipeline
  config, and OCI is the distribution path when we add remote sourcing. Running fetched code is a
  supply-chain surface we do not open yet.
- **Failure: one `on_error` policy** (`skip` | `error`, mirroring `map`). If a module crashes
  (traps), runs past its time or memory budget, or returns output we can't decode, `skip` drops the
  record and meters it (`records.dropped{reason=invalid}`) while `error` fails the node.

## Consequences

- The escape hatch opens with a trivial host and our own codec; a bad module is contained (no host
  access, bounded CPU and memory) and, under `skip`, cannot stop the pipeline.
- wasmtime + cranelift add real compile time and binary size. A later option is to ship modules
  precompiled (wasmtime's `.cwasm` format) and load them without the compile step.
- Host and guest must serialize an identical `Record`, so we factor `Record` into a shared crate and
  the SDK cannot drift from the host.
- Marshalling copies the record into the guest per call (unavoidable - it must live in the guest's
  own linear memory); the output is decoded in place, with no copy back. Instance reuse and a
  reused encode buffer keep it cheap. Going truly zero-copy would need an archived format (rkyv,
  FlatBuffers) the guest reads in place instead of an owned `Record`, trading that ergonomics for
  the last copy; deferred - measure first.
- Component Model deferred: no typed cross-language bindings yet, and the `#[non_exhaustive]`
  untagged `AttrValue` (fine for MessagePack) is exactly what makes a WIT mapping awkward today.
