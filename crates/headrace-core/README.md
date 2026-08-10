# headrace-core

[![crates.io](https://img.shields.io/crates/v/headrace-core?style=flat-square)](https://crates.io/crates/headrace-core)
[![docs.rs](https://img.shields.io/docsrs/headrace-core?style=flat-square)](https://docs.rs/headrace-core)

The engine behind [Headrace](https://headrace.rs), an OTel-native stateful stream processor.
Most users want the [`headrace`](https://crates.io/crates/headrace) binary; this is the library
it is built on:

- the internal `Record` model (the OTel data model, flattened);
- the `Backend` trait for edges between nodes (in-process today, NATS JetStream next);
- transforms (`filter`, `window`, `map`, `join`) and the pipeline runtime with static validation;
- a `Metrics` boundary so the core records its own telemetry without depending on any SDK.

Optional features: `otlp` (OTLP gRPC source/sink) and `inspect` (the state-inspection gRPC API).

Docs at [headrace.rs/docs](https://headrace.rs/docs); design in
[DESIGN.md](https://github.com/headrace-rs/headrace/blob/main/DESIGN.md).

## License

Apache-2.0.
