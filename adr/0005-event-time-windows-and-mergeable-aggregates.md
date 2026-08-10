# 0005. Event-time windows with mergeable aggregates

- Status: Accepted
- Date: 2026-08-03

## Context

Correct rollups under lag, replay, and out-of-order arrival require event-time semantics, not
wall-clock. Scaling splits a window's state across workers, so partial results must combine
without loss.

## Decision

We will window on event time (OTel `TimeUnixNano`) with watermarks for closing, and every
aggregate will be a mergeable monoid (partial + partial = total). Quantiles use mergeable
sketches (DDSketch / t-digest), not raw retention. v0.1 triggers on processing time; watermarks
follow.

## Aggregate catalog

Every aggregate must be a mergeable monoid, so the roadmap is organized by the accumulator or
sketch that preserves that property, not by user demand alone. Shipped and planned:

| Aggregate | Accumulator / sketch | Merges via | Status |
|---|---|---|---|
| `count` | `u64` | `+` | shipped |
| `sum` | `f64` | `+` | shipped |
| `min` / `max` | `f64` | `min` / `max` | shipped |
| `avg` | `(sum, count)` | componentwise `+` | shipped (derived; never merged from avgs) |
| `p50` / `p90` / `p95` / `p99` | DDSketch | sketch merge | planned (next) |
| `stddev` / `variance` | `(count, sum, sum_sq)` | componentwise `+` | planned |
| `count_distinct` | HyperLogLog | HLL union | planned |
| `first` / `last` (by event time) | `(value, ts)` | keep smallest / largest `ts` | planned |
| `histogram` (OTel exponential) | native exp-histogram buckets | bucket-wise merge | planned |
| `top_k` / heavy hitters | Count-Min / SpaceSaving | sketch merge | later |

Notes that fix the choices:

- **DDSketch over t-digest** for quantiles: bounded *relative* error is the right guarantee for
  latency, and two sketches merge exactly.
- **`avg` and `stddev`/`variance` derive from additive moments** (`count`, `sum`, `sum_sq`), each a
  monoid; we never merge an average from averages (see Consequences).
- **`first`/`last`** rely on event-time tie-breaking, which records already carry (`ts_nanos`); the
  merge keeps the sample with the smallest / largest `ts`. `last` gives a gauge-style current value.
- **OTel exponential histograms are themselves mergeable**; ingesting and merging them per window
  preserves fidelity instead of re-bucketing - directly on the OTel-native bet, and a differentiator.
- **`rate` is derived, not a first-class aggregate**: with cumulative-to-delta normalization on
  ingest, `rate = windowed sum(delta) / window duration`, expressible as a `map` over a `sum`.
  Settle aggregate-vs-derived when quantiles land.

**Explicitly out of scope** (they break the monoid): exact median, exact distinct count, exact
percentiles. They need unbounded retention and do not merge across partitions. Quantiles and
distinct counts are sketch-approximated *on purpose*, with bounded error - a design choice, not a gap.

## Consequences

- Cross-partition rollups and changelog-based recovery are correct by construction; guarded by
  property tests.
- Cumulative-versus-delta metric temporality must be normalized on ingest, or sums are wrong.
- Averages are not mergeable from averages, so we keep sum + count.
- New aggregates land only with their mergeable accumulator/sketch and a monoid property test; a
  non-mergeable statistic is rejected or offered only as a bounded-error sketch.
