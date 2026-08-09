---
title: join
description: Combine several windowed inputs into one series for cross-series arithmetic.
showAskAi: false
---

# Join

The `join` transform combines several windowed inputs into one series - the basis for
cross-series arithmetic like `a - b` or `errors / total`. It aligns its inputs on their
shared labels and window, then either reduces them with an expression or hands the aligned
values to a downstream `map` / `wasm`.

```yaml
transforms:
  - type: join
    id: latency_diff
    inputs: [checkout_p99, cart_p99]   # any number of windowed inputs
    name: "latency.diff"               # output metric name (optional)
    value: "checkout_p99 - cart_p99"   # reduce the inputs (optional)
```

## Alignment

Join matches records across inputs by `(group_by labels, window [start, end))`. For that to
work its inputs must be **windows at the same size**: windows are epoch-aligned
(`start = k * size`), so equal sizes produce identical bounds no matter when events arrived.
The alignment key is inferred from the inputs' shared `group_by`, which must be identical -
the join does not redeclare it.

A `(labels, window)` bucket **fires** the moment every input has supplied a value. An
incomplete bucket - one input had no data for that key and window - is evicted once every
input has advanced past it (the join watermark, the minimum of each input's newest window
end); its partial values are dropped and counted on `headrace.records.dropped`.

```
input hi:  [0,5s)=214   [5s,10s)=250
input lo:  [0,5s)=190   [5s,10s)=210
join:      [0,5s): hi=214, lo=190 -> emit    [5s,10s): hi=250, lo=210 -> emit
```

## Reduce, or carry

- **With `value`** (an expression over the input ids, see [map](/transforms/map) for the
  grammar): join computes the output value and emits a clean record - labels, window, and the
  computed `value`. Sink-ready.
- **Without `value`** (align-only): join carries each input's value as an **attribute named by
  its input id**, leaving the computation to a downstream `map` or `wasm`. Such a join must feed
  a transform, not a sink (its records have no computed value) - `validate` enforces this.

```yaml
# align only, then reduce in wasm
- { type: join, id: aligned, inputs: [a, b, c] }
- { type: wasm, id: combine, input: aligned }   # reads a, b, c attributes (roadmap)
```

## Scaling

The alignment key is also the backend partition key, so every input's records for a given key
land on the same worker: the join's buffered state stays local, with nothing moved between
workers (ADR-0007, ADR-0012).

## Validation

`headrace validate` rejects a join whose inputs are not windows, whose inputs disagree on
`group_by` or window size, or that is align-only yet feeds a sink.
