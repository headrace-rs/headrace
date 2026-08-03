# headrace-core

The Headrace engine:

- the internal `Record` model (the OTel data model, flattened);
- the `Backend` trait for edges between nodes (in-process today, NATS JetStream next);
- transforms (`filter`, `window`) and the pipeline runtime with static validation;
- a `Metrics` boundary so the core records its own telemetry without depending on any SDK.

See [DESIGN.md](../../DESIGN.md).
