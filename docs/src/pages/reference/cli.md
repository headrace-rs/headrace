---
title: CLI
description: The headrace command line - run, validate, schema, inspect, and global flags.
showAskAi: false
---

# CLI reference

```
headrace [GLOBAL FLAGS] <command> [ARGS]
```

## Commands

### run

```sh
headrace run <file> [--inspect-addr ADDR] [--backend nats --nats-url URL]
```

Load a pipeline and run it until Ctrl-C. `--inspect-addr` (e.g. `127.0.0.1:4318`) serves the
state-inspection gRPC API so [`inspect`](#inspect) can query live node state; it is off by
default and exposes raw state, so bind a trusted network only.

The default backend is `in-process` (in-memory channels, a single process). `--backend nats`
carries records over NATS JetStream for a durable, scaled deployment and needs `--nats-url`
(e.g. `nats://127.0.0.1:4222`); `--name` namespaces the NATS subjects (default: the pipeline
file stem). Scale out by splitting each edge into `--partitions` partitions (default 12) and
running `--workers` copies, each with a distinct `--worker-index` in `0..workers` (or the
`HEADRACE_WORKER_INDEX` env var). A key routes to `hash(key) % partitions` and worker `i` owns
the partitions where `p % workers == i`, so all state for a key stays on one worker.

### validate

```sh
headrace validate <file>
```

Parse and statically check a pipeline, then print `ok`. Catches unknown fields, unresolved
`input`s, duplicate ids, and transform-specific rules before anything runs. See
[Troubleshooting](/troubleshooting) for the errors it reports.

### schema

```sh
headrace schema
```

Print the pipeline IR as a JSON Schema - the contract for editors and code generators.

### inspect

```sh
headrace inspect <addr> [--node ID]... [--watch]
```

Query a running pipeline's live state (it must have been started with `run --inspect-addr`).
Prints each stateful node's open groups - labels, window bounds, current value, and sample
count. `--node` restricts the query to specific ids and repeats; omit it for all stateful
nodes. `--watch` streams snapshots as state changes, instead of a one-shot query, until
Ctrl-C. See [State inspection](/state-inspection) for the full guide.

## Global flags

These apply to every command.

| Flag | Default | Meaning |
|---|---|---|
| `--log <filter>` | `info` | Log filter, e.g. `info` or `headrace_core=debug`. |
| `--log-format <text\|json>` | `text` | Log output format. Logs always go to stderr. |
| `--metrics <off\|stdout\|otlp>` | `off` | Self-telemetry exporter (see [Self-metrics](/reference/metrics)). |
| `--otlp-endpoint <URL>` | - | OTLP endpoint for `--metrics otlp`; else `OTEL_EXPORTER_OTLP_ENDPOINT` / the default. |

## Examples

```sh
# run the bundled example (generator -> filter -> 5s window -> stdout)
headrace run examples/latency.yaml

# validate before shipping
headrace validate pipeline.yaml

# run with self-telemetry exported over OTLP, debug logs as JSON
headrace --metrics otlp --log-format json run pipeline.yaml

# run with the state API open, then inspect one node from another shell
headrace run pipeline.yaml --inspect-addr 127.0.0.1:4318
headrace inspect 127.0.0.1:4318 --node windowed
```
