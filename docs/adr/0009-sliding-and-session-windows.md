# 0009. Sliding and session windows, with lateness and staleness

- Status: Accepted
- Date: 2026-08-04

## Context

ADR-0005 established event-time windows with mergeable aggregates, and v0.1 ships tumbling
windows. Real workloads also need overlapping and activity-based windows, and an unbounded
keyspace needs a way to release memory for keys that stop producing.

## Decision

We will add two window kinds on top of ADR-0005:

- **sliding**: overlapping fixed windows (step < size), so a record contributes to several active
  windows.
- **session**: gap-based windows that merge while events keep arriving for a key and close after
  an idle gap.

Each window carries an **allowed lateness** (how long after a window closes a late record may
still update it, per the watermark rule in ADR-0005), and keyed state carries a **staleness** TTL
that evicts idle keys. These are event-time features and land with watermarks in v0.3.

## Consequences

- Sliding holds several accumulators per key; session needs per-key merge and close logic.
- Staleness bounds memory for unbounded keyspaces and is required for session windows to release
  closed sessions.
- Both reuse the mergeable-monoid state model from ADR-0005; no new aggregate machinery.
