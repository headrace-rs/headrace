# 0015. NATS JetStream backend: streams, acks, and workers

- Status: Accepted
- Date: 2026-08-11

## Context

[ADR-0003](0003-nats-jetstream-as-the-scaled-backend.md) chose NATS JetStream as the scaled
backend (partition by subject, one durable pull consumer per partition, workers bound by
ordinal); [ADR-0008](0008-static-partition-assignment.md) chose static partition assignment for
now. Keyed state is private to a transform ([ADR-0007](0007-keyed-state-is-private-to-a-transform.md))
and a join's inputs share a `group_by`, so they land on the same partition
([ADR-0012](0012-join-transform.md)).

Those set the *what*. This ADR pins the *how* for the `Backend` implementation: the stream and
subject layout, the wire codec, the consumer and acknowledgement model (and thus the delivery
guarantee), how a worker learns its partitions, the crate feature, and how it is tested. It does
not re-open the backend choice or the assignment strategy.

The `Backend` trait is the interface a networked backend slots into: `producer(id, key_spec)` /
`consumer(id)` per node-output edge, where the producer keys each record by `key_spec` (the
downstream transform's `group_by`) and `Producer::send(rec)` publishes it. The in-process backend
(bounded mpsc per edge) stays the default; NATS is a second implementation.

## Decision

We will add a NATS JetStream backend behind a `nats` cargo feature (pulling `async-nats`), off by
default and selected with `--backend nats --nats-url <url>`; the default stays `in-process`.

**Build it in two stages.** Stage 1 is a single worker over JetStream - it exercises durable
transport, stream provisioning, and the ack model end to end. Stage 2 adds static partitioning
for multiple workers. Stage 1 is useful on its own (a restart-tolerant, back-pressured transport)
and shakes out the transport before partitioning is layered on.

1. **Streams and subjects.** One JetStream stream per pipeline edge (a node's output), over the
   partition subjects `hr.<pipeline>.<node>.<partition>` (one partition per single worker). Headrace
   ensures its streams on startup (idempotent). Retention is **work-queue**: a record leaves once
   its single per-partition consumer acks it, matching the one-consumer-per-output rule
   (`MultipleConsumers`). The `<pipeline>` token, from `--name` (default: the file stem), namespaces
   subjects so pipelines can share a cluster.

2. **Wire codec: MessagePack** (`rmp-serde` over the existing `Record` serde model). It is compact
   and fast, and self-describing, so a `Record` that gains a field still decodes on a peer worker
   during a rolling upgrade. JSON was the debuggable but fat option; `bincode` is smaller and
   faster still but not self-describing, so it is brittle across versions - held in reserve for
   when same-version delivery is guaranteed.

3. **Consumers are durable pull consumers; delivery is at-least-once with ack-after-processing.** A
   node applies a record to its state synchronously on receipt, so the consumer acks the *previous*
   message when the node pulls the next one (and acks the last on drain). This keeps
   `Consumer::recv() -> Option<Record>` unchanged - the ack is internal to the NATS consumer, and a
   no-op in-process - rather than threading an ack handle through every node loop, which we reject
   as invasive. On a crash, unacked records are redelivered.

4. **Static partition assignment (stage 2).** A stream has a fixed partition count `P`
   (`--partitions`, default 12); its subject carries the partition. A record maps to `hash(key) % P`
   (fixed key-groups, the Flink model of ADR-0008) where `key` is the downstream transform's
   `group_by`. Headrace hashes it **client-side** (a stable FNV-1a) and publishes to the partition
   subject, rather than via NATS's `partition` subject-mapping: no server-side transform to
   configure, and the partition math stays a pure, unit-tested function. Worker `i` of `N`
   (`--worker-index` / `--workers`, or a StatefulSet ordinal) binds a consumer per partition where
   `p % N == i` (`N <= P`). A key maps to the same `p` on every edge, so a join's inputs meet on one
   worker with no cross-worker movement.

   We keep fixed key-groups rather than a consistent-hash (ketama) ring: a ring maps keys straight
   to workers and remaps individual keys whenever `N` changes, whereas fixed key-groups reassign
   whole partitions, bounding how much state moves on a rescale. Consistent hashing / rendezvous is
   deferred to the checkpointing era, per ADR-0008.

5. **Testing.** Partition math and subject naming are pure and unit-tested. A `docker`-gated
   integration test runs `nats` with JetStream via testcontainers in its own CI job; the default
   `cargo test` needs no server.

## Consequences

- The first external dependency. The `nats` feature keeps it optional, so the default and edge
  builds stay a single self-contained binary; only the scaled image enables it.
- The delivery guarantee is **at-least-once transport, not exactly-once**. Node state is in-memory
  and not checkpointed, so a crash loses a worker's open windows and buckets even as JetStream
  redelivers unacked records. Durable state (a compacted changelog) is v0.5; this ADR does not
  attempt it. v0.4 provides durable, back-pressured transport plus the partitioning substrate for
  horizontal scale.
- `Backend` / `Consumer` signatures are unchanged; the ack lives inside the NATS consumer. The
  trait comment's promise ("acks arrive with the durable NATS backend") holds without churn.
- Ops gain a dependency: a NATS cluster with JetStream enabled. Headrace provisions its own
  streams, so the only manual step is pointing `--nats-url` at the cluster.
- Static assignment means changing `N` needs a restart and does not migrate state; the accepted
  trade until checkpointing makes migration cheap (ADR-0008).
