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

v0.1: IR, in-process backend, `generator`/`stdin` → `filter`/`window` → `stdout`.
Next: OTLP in/out, WASM transform, NATS JetStream backend, Helm chart, MCP authoring server.

Apache-2.0.
