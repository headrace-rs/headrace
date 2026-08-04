# 0010. Cross-series arithmetic

- Status: Proposed
- Date: 2026-08-04

## Context

Users want to derive one series from several: `metric_a - metric_b`, ratios, error rates
(`errors / total`). The `window` transform reduces a *single* series (`group_by` + one op over
`value`); it cannot combine two. Combining needs (a) alignment of the inputs by shared labels and
the same window, and (b) an arithmetic step. Records are keyed and partitioned by `group_key`, and
state is private to a transform (ADR-0007), so any design must keep the combine local to one
partition.

Prior art frames two shapes:

- **PromQL** - `a - b` is a binary op over two instant vectors, matched by label set
  (`on(...)` / `ignoring(...)`), evaluated per step. Alignment by labels is implicit; there is no
  explicit join in the query.
- **Flink / Kafka Streams** - an explicit `join` co-partitioned on a key, then a `map`/expression
  over the joined record. Explicit, and locality falls out of co-partitioning.

## Decision (proposed)

Provide it in two layers, which is what the roadmap's `map` + `join` (v0.3) are for:

- A **`join`** transform takes two inputs co-partitioned on the shared `group_by`, buffers each
  side per `(group_key, window)`, and emits a combined record when both sides are present. This is
  the mechanism, and it keeps the combine on one worker (ADR-0007). Join nodes make the IR's single
  `input` grow to `inputs: [a, b]`.
- A **`map`/expression** transform then computes the value (`a - b`, `a / b`) as a small closed
  expression over the joined fields - not arbitrary code, which is what `wasm` is for.

A PromQL-style *surface* (a binary op matched by labels, no explicit join in the IR) is attractive
for authoring, and is the likely ergonomic layer an agent targets - but it compiles **down to**
join + map. We defer committing to that surface syntax until join + map exist.

## Consequences

- Cross-series math is a v0.3 feature, gated on `join` (itself gated on co-partitioning both
  inputs) and on event-time windows, so "the same window" is well defined.
- Inputs must be label-aligned (shared group key), time-aligned (same window), and comparable
  (same temporality and unit). A mismatch is a validation or runtime error, never silent garbage.
- Multi-input nodes change the IR (`inputs` vs `input`) and validation (a join's two inputs must be
  co-partitioned). Fan-in arrives with join; fan-out (`tee`) stays a separate concern.
- A single-input "vector math over one stream carrying both metrics by name" is a special case of
  join + map; we prefer the general mechanism and can add that convenience later.
