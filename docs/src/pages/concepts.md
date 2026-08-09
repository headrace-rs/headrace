---
title: Concepts
description: The pipeline graph, the record in flight, event time, and run modes.
showAskAi: false
---

# Concepts

Four ideas cover most of headrace: the **pipeline** you declare, the **record** that flows
through it, the **event time** that windows reason about, and the **run mode** you deploy in.

## The pipeline

A pipeline is a directed graph of nodes, written as YAML. It has three node kinds:

```yaml
version: 1          # optional, defaults to 0
sources: [...]      # where records enter (required)
transforms: [...]   # how records are reshaped (optional)
sinks: [...]        # where records leave (required)
```

Every node has an `id`. Transforms and sinks name their upstream by `input` (a `join` names
several, as `inputs`), so the ids wire the graph together:

```yaml
sources:
  - { type: otlp, id: in, listen: 0.0.0.0:4317 }
transforms:
  - { type: filter, id: checkout, input: in, key: http.route, equals: /checkout }
sinks:
  - { type: stdout, id: out, input: checkout }
```

`headrace validate` checks the graph statically before it runs: every `input` resolves to a
real node, ids are unique, each output feeds at most one consumer (fan-out is not allowed -
`join` is the only fan-in), and every transform's requirements hold (see
[Troubleshooting](/troubleshooting#validation-errors)).

## The record

The unit in flight is a `Record` - the OTel data model flattened to what nodes need:

| Field | Meaning |
|---|---|
| `name` | the metric name |
| `value` | the sample, an `f64` |
| `ts_nanos` | event time (OTel `TimeUnixNano`); for an aggregate, the window end |
| `start_ts_nanos` | window start (OTel `StartTimeUnixNano`); set by windowing, else unset |
| `resource` | resource-level attributes (e.g. `service.name`) |
| `scope` | the instrumentation scope, if any |
| `attrs` | record-level attributes (e.g. `http.route`) |

Attributes are OTel `AnyValue`: bool, int, double, or string. A **numeric field** referenced
by `map`, `window`, or `filter` resolves like this: `value` (or an omitted field) is the
record's own `value`; any other name is looked up in `attrs`, then falls back to `resource`.
An absent field and a present-but-non-numeric one are distinguished - that is what the
`on_missing` / `on_invalid` policies act on.

## Event time

Windowing and joins place records by **event time** - each record's own `ts_nanos`, not the
wall clock - so the aggregates stay correct under lag, batching, and replay. A **watermark**
tracks how far event time has advanced; a window fires when the watermark passes its end. The
[window](/transforms/window) transform is built entirely on this - read it there for the full
picture.

## Run modes

The same pipeline runs two ways, chosen by the backend, not the YAML:

- **In-process** (today): one binary, state held in memory, no external dependencies. Ideal
  for dev, the edge, and single-pod Kubernetes. This is what `headrace run` uses.
- **Partitioned backend** (roadmap): a stateless front end tags each record with its
  `group_by` key and hands it to a partitioned backend (NATS or Kafka), which routes every
  record to the worker that owns its key. Same IR, scaled across a cluster.

OTLP is how data gets in and out; the backend is how records move between nodes internally.
Because a stateful node's partition key is its `group_by`, every record for a given key lands
on the same worker, so that worker keeps all of the key's state locally - no coordination
between workers.
