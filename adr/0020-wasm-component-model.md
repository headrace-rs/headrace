# 0020. WASM transform: adopt the Component Model

- Status: Accepted (supersedes the interface and authoring of ADR-0018)
- Date: 2026-08-19

## Context

ADR-0018 shipped the `wasm` transform on a hand-rolled core-wasm bytes ABI: the host and guest pass
a `Record` as MessagePack over linear memory, and a Rust-first SDK (`#[transform]`) emits the
`alloc`/`dealloc`/`transform` exports. Every other language must hand-write that glue and its own
`Record` codec - a maintenance burden and a drift surface. ADR-0018 named the untagged `AttrValue`
as the specific thing that made a typed interface awkward. With more than one target language
wanted, per-language glue does not scale.

## Decision

- **Adopt the WebAssembly Component Model** (WIT + `wit-bindgen`), superseding ADR-0018's bytes ABI
  and its Rust-first authoring.
- **Define the interface in WIT.** A `record` type; `attr-value` as a WIT `variant` (the typed
  replacement for the untagged enum that blocked this before); a world exporting
  `transform: func(rec: record) -> list<record>`. An empty list drops; several records fan out -
  the logical contract ADR-0018 promised to hold stable.
- **Host uses wasmtime's component model.** Enable the `component-model` feature and generate host
  bindings with `component::bindgen!`. The canonical ABI does the lowering and lifting, so the
  hand-written marshalling and the MessagePack codec go away. The ADR-0018 sandbox (empty imports,
  epoch CPU budget, `StoreLimits`) and the `on_error` policy carry over unchanged.
- **Guests use `wit-bindgen`.** One WIT generates bindings for Rust, Go, JS, C, and Python, so
  polyglot authoring needs no per-language SDK and cannot drift from the host.
- **Version the interface with WIT semver**, replacing the ad-hoc `ABI_VERSION` integer.

## Consequences

- This replaces the bytes ABI, the MessagePack marshalling, the shared-`Record` codec path, and the
  bytes-ABI `headrace-wasm-guest` SDK. It is a v2 of the transform; the bytes ABI shipped in 0.1.20,
  so it breaks any module built against it - acceptable at 0.x, with the SDK one release old and few
  users. The sandbox, resource limits, `on_error`, and OCI sourcing (ADR-0019) still apply.
- The `component-model` feature adds build weight over the core-wasm runtime.
- Polyglot authoring falls out of one interface definition, which is the reason for the switch.

## Scope: wasm transforms stay pure compute

wasm modules get no network or host I/O - the ADR-0018 sandbox stands. External enrichment (calling
an API or a store per record) is deliberately out of scope for the transform: per-record synchronous
I/O blocks the worker and fights back-pressure. Enrichment, when we need it, is a dedicated stateful
node (caching, batching) or a sidecar, not network-in-wasm.
