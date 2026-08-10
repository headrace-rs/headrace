<p align="center">
  <img src="./brand/assets/avatar.svg" alt="Headrace" width="84">
</p>

# Headrace

[![CI](https://img.shields.io/github/actions/workflow/status/headrace-rs/headrace/ci.yml?branch=main&style=flat-square&label=CI)](https://github.com/headrace-rs/headrace/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/codecov/c/github/headrace-rs/headrace/main?style=flat-square)](https://codecov.io/gh/headrace-rs/headrace)
[![crates.io](https://img.shields.io/crates/v/headrace?style=flat-square)](https://crates.io/crates/headrace)
[![docs](https://img.shields.io/badge/docs-headrace.rs-e76125?style=flat-square)](https://headrace.rs/docs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](./LICENSE)

OTel-native, stateful stream processing in a single Rust binary. Point OpenTelemetry at it,
declare aggregations in YAML, and forward the aggregated metrics to any backend - not every
datapoint. Runs in-process for dev and the edge, or scales on Kubernetes over a partitioned
backend.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./docs/assets/pipeline-dark.svg">
  <img src="./docs/assets/pipeline.svg" alt="headrace pipeline: sources through stateful transforms to sinks" width="100%">
</picture>

## Install

```sh
cargo install headrace                                            # from crates.io
cargo binstall headrace                                           # prebuilt binary, no compile
docker pull ghcr.io/headrace-rs/headrace                          # container image
helm install headrace oci://ghcr.io/headrace-rs/charts/headrace   # Kubernetes
```

Prebuilt binaries for Linux and macOS are attached to each
[release](https://github.com/headrace-rs/headrace/releases); full options at
[headrace.rs/docs/install](https://headrace.rs/docs/install).

## Quickstart

Run the bundled example - a generator feeding a filter and a 5s window, printing aggregates
to stdout:

```sh
cargo run -p headrace -- run examples/latency.yaml
```

Validate a pipeline, or print the IR JSON Schema:

```sh
cargo run -p headrace -- validate examples/latency.yaml
cargo run -p headrace -- schema
```

## Features

| Kind | Supported | Planned |
|---|---|---|
| Sources | `otlp` (gRPC receiver), `generator`, `stdin` | - |
| Transforms | `filter`, `window` (tumbling + sliding, event-time), `map`, `join` (cross-series) | session windows, `wasm` |
| Sinks | `otlp` (gRPC exporter), `stdout` (text / json) | Prometheus remote-write |
| Aggregates | `count`, `sum`, `min`, `max`, `avg` | quantiles (DDSketch), `stddev`, `count_distinct` (HLL), `first`/`last`, OTel exp-histogram merge - all mergeable ([ADR-0005](./adr/0005-event-time-windows-and-mergeable-aggregates.md)) |
| Backend | in-process (single binary) | NATS JetStream (partitioned, scaled) |
| Deploy | Helm chart, OTLP `Record` cumulative-to-delta normalization | `Pipeline` CRD + operator |

## Documentation

Full docs at [headrace.rs/docs](https://headrace.rs/docs):

- [Install](https://headrace.rs/docs/install) and [Getting started](https://headrace.rs/docs/getting-started)
- [Concepts](https://headrace.rs/docs/concepts) - the pipeline graph, the record, event time, run modes
- Pipeline nodes: [sources](https://headrace.rs/docs/sources), transforms
  ([filter](https://headrace.rs/docs/transforms/filter),
  [map](https://headrace.rs/docs/transforms/map),
  [window](https://headrace.rs/docs/transforms/window),
  [join](https://headrace.rs/docs/transforms/join)), [sinks](https://headrace.rs/docs/sinks)
- Reference: [CLI](https://headrace.rs/docs/reference/cli),
  [self-metrics](https://headrace.rs/docs/reference/metrics)
- [Troubleshooting](https://headrace.rs/docs/troubleshooting)
- [DESIGN.md](./DESIGN.md) - how it works, end to end

## Self-metrics

Headrace exports its own telemetry over OTLP - the same protocol it processes - attributed by
`node` and `kind`. Off by default; enable with `--metrics otlp` (or `stdout` for debugging).
See [self-metrics](https://headrace.rs/docs/reference/metrics) for the full instrument list.

## Status

v0.1 shipped the in-process pipeline: IR with static validation, `filter` and tumbling
`window`, `generator`/`stdin` sources, `stdout` sinks, and OTel self-metrics. v0.2 has
since added the OTLP source/sink (behind the `otlp` feature), event-time windows with
watermarks and `allowed_lateness`, and a Helm chart. v0.3 adds the `map` and `join`
transforms and local state inspection - a `State` gRPC API (`Get` and streaming `Watch`)
served by `run --inspect-addr`, with a `headrace inspect` client.

Roadmap (details in [DESIGN.md](./DESIGN.md#roadmap)) - core processing first, on the
in-process backend and deployable in a real cluster, before a distributed backend:

- **v0.2 - deployable core**: OTLP in/out, Helm chart + GHCR, event-time windows +
  watermarks, sliding windows.
- **v0.3 - richer transforms**: `map`, `join`, local state inspection.
- **v0.4 - scale & extend**: NATS JetStream backend, WASM transform.
- **v0.5+**: state checkpointing + interactive queries, MCP authoring server, `Pipeline`
  CRD + operator, session windows.

Apache-2.0.
