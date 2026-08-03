# headrace-ir

The Headrace pipeline IR: typed, `serde`-serializable types that describe a pipeline
(sources, transforms, sinks) and emit a JSON Schema (`headrace schema`).

This is the *configuration* schema an author or agent targets. It is not the data model;
records in flight are the OTel-shaped `Record` in `headrace-core`.

No runtime dependencies. See [DESIGN.md](../../DESIGN.md).
