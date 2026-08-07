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

## Consequences

- Cross-partition rollups and changelog-based recovery are correct by construction; guarded by
  property tests.
- Cumulative-versus-delta metric temporality must be normalized on ingest, or sums are wrong.
- Averages are not mergeable from averages, so we keep sum + count.
