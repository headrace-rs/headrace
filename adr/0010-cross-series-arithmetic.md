# 0010. Cross-series arithmetic

- Status: Accepted
- Date: 2026-08-06

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

## Decision

Provide it in two layers - `map` (v0.3, first) then `join` (v0.3, next):

- A **`map`/expression** transform rewrites a record's `value` from a **closed numeric
  expression**: the binary operators `+ - * / %` and `^` (power), unary `-`, parentheses, number
  literals, the keyword `value`, and bare references to numeric attributes - e.g. `errors / total`,
  `value / 1000`, `a - b`. It is deliberately closed (no I/O, branching, or string ops) so it parses
  and validates statically and evaluates cheaply. A missing or non-numeric field, and a non-finite
  result, follow the window's `on_missing: skip | error`. Functions (`min`, `sqrt`, ...) and more
  operators come later (~v0.7); arbitrary per-record logic is what **`wasm`** (v0.4) is for.
- A **`join`** transform takes two inputs co-partitioned on the shared `group_by`, buffers each
  side per `(group_key, window)`, and emits a combined record when both sides are present. It keeps
  the combine on one worker (ADR-0007) and grows the IR's single `input` to `inputs: [a, b]`.
  Cross-series math is then `join` (align a and b in the same window) followed by `map` (`a - b`).

A PromQL-style *surface* (a binary op matched by labels, no explicit join in the IR) is attractive
for authoring and compiles **down to** join + map; we defer that surface until both exist. A
rename/remap of record keys (e.g. the metric name) is a related authoring convenience, also
deferred.

## Consequences

- Cross-series math is a v0.3 feature, gated on `join` (itself gated on co-partitioning both
  inputs) and on event-time windows, so "the same window" is well defined.
- Inputs must be label-aligned (shared group key), time-aligned (same window), and comparable
  (same temporality and unit). A mismatch is a validation or runtime error, never silent garbage.
- Multi-input nodes change the IR (`inputs` vs `input`) and validation (a join's two inputs must be
  co-partitioned). Fan-in arrives with join; fan-out (`tee`) stays a separate concern.
- A single-input "vector math over one stream carrying both metrics by name" is a special case of
  join + map; we prefer the general mechanism and can add that convenience later.
