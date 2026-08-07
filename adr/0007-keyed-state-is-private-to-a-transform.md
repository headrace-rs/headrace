# 0007. Keyed state is private to a transform

- Status: Accepted
- Date: 2026-08-04

## Context

Windowing state is keyed by `(transform_id, group_key, window)` and co-located with the
transform's keyspace partition, so a group's records and its state share a worker. If one
transform could read another's state, and the two are partitioned on different keys, every lookup
would cross partitions and workers. That per-record distributed read removes the locality that
makes stateful scaling work.

## Decision

We will keep keyed state private to the transform that owns it; no transform reads another's state
directly. To combine state across streams:

- **join** co-partitions both inputs on the join key, so both sides' state lands on the same
  worker and stays local (the Flink and Kafka-Streams model).
- **broadcast state** replicates a small, read-mostly table (rules, config, reference data) to
  every partition.

Large reference data lives in an external lookup, outside the state model.

## Consequences

- The one-key, one-worker, local-state invariant holds, so scaling stays tractable.
- Cross-stream combination is explicit: a join declares its co-partitioning (a shuffle), and
  broadcast state declares a bounded, replicated table.
- There are no general cross-transform state references; cases that seem to want them are
  expressed as a join or broadcast, or pushed to an external store.
