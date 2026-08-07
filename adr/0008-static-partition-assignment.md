# 0008. Static partition assignment; defer consistent hashing

- Status: Accepted
- Date: 2026-08-04

## Context

Workers bind partitions by StatefulSet ordinal (`partition % replicas == ordinal`), so changing
the partition count P is a rolling operation rather than a seamless rebalance. Consistent hashing
(a ketama ring) is the usual proposal to smooth that. But the cost that dominates a reassignment
is migrating a key's window state, not remapping the key; and until state checkpointing exists
(v0.3) there is no durable state to migrate, so in-flight windows are dropped and rebuilt
regardless of the hash. NATS JetStream also routes server-side by subject, so a client-side ring
would move partition assignment back into Headrace, which we push to the backend.

## Decision

We will use static partition assignment for v0.2 and defer any consistent-hashing scheme until
state checkpointing (v0.3) makes migration cheap. When we revisit assignment, we will evaluate
key-groups (Flink) or rendezvous hashing rather than a plain ring, because those bound how much
state moves on a scale event.

## Consequences

- Scaling P stays a rolling operation in v0.2, which is acceptable at this stage.
- No client-side assignment logic to build now; assignment stays the backend's job.
- The assignment strategy is revisited together with checkpointing, not before it.
