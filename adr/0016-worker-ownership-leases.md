# 0016. Worker ownership leases: fail fast on a duplicate index

- Status: Accepted
- Date: 2026-08-15

## Context

Static assignment ([ADR-0008](0008-static-partition-assignment.md), implemented in
[ADR-0015](0015-nats-jetstream-backend.md)) gives worker `i` of `N` the partitions where
`p % N == i`, set by `--worker-index`. Correctness needs each index held by exactly one worker. A
StatefulSet ordinal guarantees that; a mistyped flag or two runs sharing a `--name` do not.

A duplicate index is silent and wrong. Two workers at the same index bind the same per-partition
durables, so JetStream splits a key's records across both (each aggregates a fragment), while the
absent index's partitions have no consumer and stall. Dynamic reassignment would fix the setup, but
moving a partition's window state is lossy until checkpointing (v0.5, ADR-0008). So we guard the
assignment rather than replace it.

## Decision

We will lease each worker index in NATS KV, which reuses the JetStream we already need.

- At startup a worker create-claims the key for its index in a per-pipeline bucket
  (`hr_<pipeline>_workers`). A held key means another worker owns the index, and startup errors. A
  transient NATS error retries; only a real conflict is fatal.
- The bucket has a TTL and the worker renews within it, so a live worker keeps the lease and a dead
  one's expires, freeing the index after the TTL.
- This is mutual exclusion, not assignment. A worker still learns its index statically; we do not
  elect or rebalance.

## Consequences

- A duplicate index errors at startup instead of splitting state and stalling partitions.
- Adds one KV bucket per pipeline and a renewal task per worker; no new dependency.
- A crashed worker's index is reclaimable only after the TTL, so a replacement may wait that long -
  bounded, and a StatefulSet serializes replacement anyway.
- Even `--workers 1` takes a lease, so two runs on the same `--name` collide too.
- Elastic rebalance stays a v0.5 concern, once checkpointing makes moving state cheap.
