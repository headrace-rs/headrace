# 0016. Worker ownership leases: fail fast on a duplicate index

- Status: Accepted
- Date: 2026-08-15

## Context

[ADR-0008](0008-static-partition-assignment.md) chose static partition assignment, and
[ADR-0015](0015-nats-jetstream-backend.md) implemented it: worker `i` of `N` owns the partitions
where `p % N == i`, selected with `--workers` / `--worker-index` (or `HEADRACE_WORKER_INDEX`).

Correctness depends on each index being held by exactly one running worker. In Kubernetes a
StatefulSet ordinal guarantees that. Nothing else does: a fat-fingered `--worker-index`, or two
`headrace run` processes sharing a `--name`, can run two workers at the same index.

That failure is silent and damaging. Per-partition durable pull consumers are named by partition,
so two same-index workers bind the *same* durables; JetStream load-balances a durable's messages
across its pullers, so a key's records split across both workers and each computes a wrong partial
aggregate. Meanwhile the partitions of the *absent* index have no consumer, pile up in the
work-queue, stall that slice of the keyspace, and eventually back-pressure producers. Nothing errors.

We are not ready to replace static assignment with dynamic assignment (a consumer-group model):
rebalancing a stateful partition means moving its window state, which is lossy until checkpointing
(v0.5, ADR-0008). So this must *guard* static assignment, not replace it.

## Decision

We will guard worker-index uniqueness with a lease in NATS KV, which runs on the JetStream we
already require, so it adds no new infrastructure.

- On startup, before binding consumers, a worker atomically claims the key for its index in a
  per-pipeline bucket (`hr_<pipeline>_workers`) with a create-if-absent write. If the key is already
  held, startup fails with a clear error naming the index and the likely `--workers` /
  `--worker-index` misconfiguration. A transient NATS error retries (as stream provisioning does);
  only a genuine conflict is fatal.
- The bucket has a TTL and the worker renews its key on an interval, so a live worker keeps its
  lease and a crashed one's lease expires, freeing the slot for a replacement within the TTL.
- This is mutual exclusion, not assignment. Workers still learn their index statically (a
  StatefulSet ordinal in production); we do not elect a leader or rebalance.

## Consequences

- A concurrent duplicate index fails loudly at startup instead of silently splitting keyed state
  and stalling the unowned partitions - the motivating bug.
- Adds a KV bucket per pipeline (a JetStream stream underneath) and a lightweight renewal task per
  worker. No new external dependency.
- A crashed worker's slot is reclaimable only after its lease TTL expires, so a replacement started
  within that window waits (bounded by the TTL). Acceptable: a StatefulSet already serializes pod
  replacement, and the alternative is silent corruption.
- Even a single worker (`--workers 1`) takes a lease, so two stray `run --backend nats` processes
  on the same `--name` also collide and fail fast.
- Dynamic assignment / elastic rebalance stays deferred to the checkpointing era (v0.5, ADR-0008),
  where moving a partition's state becomes cheap enough to make membership changes non-lossy.
