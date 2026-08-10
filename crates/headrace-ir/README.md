# headrace-ir

[![crates.io](https://img.shields.io/crates/v/headrace-ir?style=flat-square)](https://crates.io/crates/headrace-ir)
[![docs.rs](https://img.shields.io/docsrs/headrace-ir?style=flat-square)](https://docs.rs/headrace-ir)

The pipeline IR for [Headrace](https://headrace.rs): typed, `serde`-serializable types that
describe a pipeline (sources, transforms, sinks) and emit a JSON Schema (`headrace schema`).

This is the *configuration* schema an author or agent targets. It is not the data model;
records in flight are the OTel-shaped `Record` in
[`headrace-core`](https://crates.io/crates/headrace-core).

No runtime dependencies. Docs at [headrace.rs/docs](https://headrace.rs/docs).

## License

Apache-2.0.
