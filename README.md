<p align="center">
  <img src="./brand/assets/avatar.svg" alt="Headrace" width="84">
</p>

# Headrace

[![CI](https://img.shields.io/github/actions/workflow/status/headrace-rs/headrace/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/headrace-rs/headrace/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/headrace-rs/headrace/main?style=flat-square)](https://codecov.io/gh/headrace-rs/headrace)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

OTel-native, stateful stream processing in a single Rust binary. Point telemetry at it,
define aggregations declaratively, emit to any backend. Runs in-process for dev/edge or
scales on Kubernetes over a partitioned backend.

See [DESIGN.md](./DESIGN.md).

## Try it

```sh
cargo run -p headrace -- run examples/latency.yaml   # generator -> filter -> 5s window -> stdout
cargo run -p headrace -- validate examples/latency.yaml
cargo run -p headrace -- schema                       # IR JSON Schema
cargo run -p headrace -- --metrics otlp run examples/latency.yaml   # export headrace's own metrics
```

## Status

v0.1 runs the in-process pipeline: IR with static validation, `filter` and tumbling
`window` transforms, `generator`/`stdin` sources, `stdout` sinks, and OTel self-metrics.
The OTLP source/sink (v0.2) has landed behind the `otlp` feature.

Roadmap (details in [DESIGN.md](./DESIGN.md#roadmap)) - core processing first, on the
in-process backend and deployable in a real cluster, before a distributed backend:

- **v0.2 - deployable core**: OTLP in/out ✓, Helm chart + GHCR, event-time windows +
  watermarks, sliding windows.
- **v0.3 - richer transforms**: `map`, `join`, local state inspection.
- **v0.4 - scale & extend**: NATS JetStream backend, WASM transform, docs site.
- **v0.5+**: state checkpointing + interactive queries, MCP authoring server, `Pipeline`
  CRD + operator, session windows.

Apache-2.0.
