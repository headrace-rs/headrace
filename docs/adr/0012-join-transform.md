# 0012. Join transform: align N series, optionally reduce

- Status: Accepted
- Date: 2026-08-06

## Context

ADR-0010 introduced cross-series arithmetic as a `join` (align two inputs) followed by a `map`
(compute `a - b`), and noted join makes the IR's single `input` grow to multiple inputs. Designing
it surfaced three refinements:

- Folding the arithmetic into join would couple join to the expression language, blocking other
  combines (notably `wasm`) over aligned series.
- The inputs are already grouped, and - for the scaled backend - partitioned by that same
  `group_key`. So the join's alignment key *is* the inputs' shared `group_by`, not a key the join
  re-declares.
- A join that only aligns produces a record with no single meaningful `value`; it is an
  intermediate, not a sink-ready series.

## Decision

We will add a `join` fan-in transform:

- **N-ary inputs.** `inputs: [id, ...]`, any number.
- **Alignment on `(shared group_by, window)`.** Inputs must be windowed at the **same size** -
  windows are epoch-aligned (`start = k*size`), so equal sizes yield identical `[start,end)`
  boundaries regardless of arrival time. The alignment key is **inferred** from the inputs'
  `group_by`, which must be identical (validated); the join does not re-declare it. Aligning on a
  label subset (PromQL `on()`/`ignoring()`) is deferred.
- **Output.** One record per `(key, window)`, carrying each input's value as a numeric attribute
  **keyed by the input's node id**, plus the shared `group_by` labels, the window bounds, and a
  settable `name`.
- **Computation is separate.** An optional `value: <expr>` reduces the aligned inputs to the output
  value inline (the common math case; the output is then clean and sink-ready, and the per-input
  attributes are dropped). Omit `value` to align only and let a downstream `map` (expression) or
  `wasm` (arbitrary) compute; an align-only join must feed a compute transform, not a sink
  (validated).
- **Co-partitioning.** The alignment key is the backend partition key, so all inputs' records for a
  key land on one worker and the join's buffered state stays local (ADR-0007). This holds because
  the inputs share `group_by`; no shuffle is added.
- **Non-windowed "signal" join** (align on key only, keeping each input's last value) is a later
  mode: it needs a staleness TTL and an emit-trigger policy (ADR-0009).

This amends ADR-0010: the arithmetic need not live in a separate `map`; join reduces inline when
given a `value`, and defers to `map`/`wasm` otherwise.

Separately, a `name` field is added to `window`, `map`, and `join` to rename a derived metric;
richer attribute reshaping (rename/set/drop) remains the `relabel` transform (ADR-0011).

```yaml
# sink-ready in one node
- type: join
  id: latency_diff
  inputs: [checkout_p99, cart_p99]     # windowed at the same size, same group_by
  name: "latency.diff"
  value: "checkout_p99 - cart_p99"      # reduce the aligned inputs

# align-only, then wasm
- type: join
  id: aligned
  inputs: [a, b, c]
  name: "combined"
- type: wasm                            # reads a, b, c attributes; sets value
  input: aligned
```

## Consequences

- Join is the first fan-in node: the runtime's one-input-per-transform wiring generalizes to N
  consumers, and validation gains input co-partitioning (identical `group_by`) and the
  align-only-must-feed-a-compute rule.
- Reusing the `attrs` map to carry per-input values avoids a new Record field, but those transient
  entries can leak downstream on the align-only path until the inline `value` drops them or a
  `relabel`/`wasm` does.
- Non-math combines compose after an align-only join, so join never becomes a dead-end.
- Distributed joins are local rather than a shuffle, because the alignment key is the partition
  key.
