---
title: Self-metrics
description: Headrace's own telemetry, exported over the same OTLP it processes.
showAskAi: false
---

# Self-metrics

Headrace exports its own telemetry over OTLP - the same protocol it processes - so you can
watch a pipeline with the tools you already point at everything else. It is **off by default**.

## Enabling

```sh
headrace --metrics otlp run pipeline.yaml          # export over OTLP/gRPC
headrace --metrics otlp --otlp-endpoint http://collector:4317 run pipeline.yaml
headrace --metrics stdout run pipeline.yaml        # print to stderr, for debugging
```

- **`otlp`** exports to `--otlp-endpoint`, or `OTEL_EXPORTER_OTLP_ENDPOINT`, or the default.
- **`stdout`** periodically prints metrics to stderr. Convenience only: the dump interleaves
  with a stdout sink's data, so prefer `otlp` for clean output.

## Instruments

Every metric is attributed by `node` (the node id) and `kind` (`source`, `filter`, `window`,
`map`, `join`, `sink`); `records.dropped` adds a `reason`. The two window instruments carry
`node` only.

| Metric | Type | Attributes | Meaning |
|---|---|---|---|
| `headrace.records.out` | counter | node, kind | Records a node emitted or forwarded. |
| `headrace.records.dropped` | counter | node, kind, reason | Records dropped, split by `reason` (below). |
| `headrace.window.flushes` | counter | node | Window flush events. |
| `headrace.window.groups` | histogram | node | Aggregate groups emitted per flush. |
| `headrace.node.errors` | counter | node, kind | Node tasks that terminated with an error. |

`reason` on `headrace.records.dropped`:

| `reason` | Meaning |
|---|---|
| `filtered` | A `filter` predicate rejected the record. |
| `invalid` | A missing/non-numeric field or an inevaluable expression, under a `skip` policy. |
| `late` | The record arrived after its window had already fired. |
| `incomplete` | A `join` bucket was evicted before every input supplied a value. |
| `capped` | The node was at its `max_groups` cap. |

## Reading them

- A rising `records.dropped{reason=late}` means `allowed_lateness` is too small for the
  source's out-of-orderness - see [Troubleshooting](/troubleshooting#late-records).
- `records.dropped{reason=invalid}` on a `window` or `map` node points at a missing or
  non-numeric field with an `on_missing` / `on_invalid` policy of `skip`.
- `headrace.window.groups` shows the cardinality of each flush - useful for spotting a
  `group_by` that fans out more than you expected.
- `records.dropped{reason=capped}` is nonzero only when a node hit its `max_groups` cap: the
  `group_by` cardinality outran the limit (a misconfigured key or an attack). Raise the cap or
  narrow the `group_by`.
