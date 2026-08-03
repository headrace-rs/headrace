# Headrace

OTel-native, stateful stream processing in a single Rust binary. Point telemetry at it,
define aggregations declaratively, emit to any backend. Runs in-process for dev/edge or
scales on Kubernetes over a partitioned backend.

See [DESIGN.md](./DESIGN.md).

## Try it

```sh
cargo run -p headrace -- run examples/latency.yaml   # generator → filter → 5s window → stdout
cargo run -p headrace -- validate examples/latency.yaml
cargo run -p headrace -- schema                       # IR JSON Schema
cargo run -p headrace -- --metrics otlp run examples/latency.yaml   # export headrace's own metrics
```

## Status

v0.1 (current) runs the in-process pipeline: IR with static validation, `filter` and
tumbling `window` transforms, `generator`/`stdin` sources, `stdout` sinks, and OTel
self-metrics.

Roadmap (details in [DESIGN.md](./DESIGN.md#roadmap)):

- **v0.1**: in-process backend; `filter` + tumbling `window`; `stdout`/JSON sinks.
- **v0.2**: OTLP in/out, WASM transform, NATS JetStream backend, Helm chart, MCP authoring
  server, docs site.
- **v0.3**: state checkpointing, event-time windows + watermarks, `map`/`join`, `Pipeline` CRD.

Apache-2.0.
