# Architecture Decision Records

Significant architectural decisions are recorded here as ADRs, using the
[Nygard format](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
(see [`template.md`](./template.md)). Background: the
[ADR project](https://github.com/architecture-decision-record/architecture-decision-record).

## How to add one

1. Copy `template.md` to `NNNN-kebab-title.md`, incrementing `NNNN`.
2. Write it, set the status, and open a PR (ideally in the same PR as the change it justifies).
3. A superseded decision stays in place: mark it `Superseded by ADR-XXXX` and link both ways.

## Index

- [0001](./0001-record-architecture-decisions.md) - Record architecture decisions
- [0002](./0002-headrace-is-a-layer-not-infrastructure.md) - Headrace is a layer, not infrastructure
- [0003](./0003-nats-jetstream-as-the-scaled-backend.md) - NATS JetStream as the scaled backend
- [0004](./0004-otlp-at-the-boundaries-uniform-record-model.md) - OTLP at the boundaries, uniform Record model
- [0005](./0005-event-time-windows-and-mergeable-aggregates.md) - Event-time windows with mergeable aggregates
- [0006](./0006-call-processing-nodes-transforms.md) - Call processing nodes "transforms"
- [0007](./0007-keyed-state-is-private-to-a-transform.md) - Keyed state is private to a transform
- [0008](./0008-static-partition-assignment.md) - Static partition assignment; defer consistent hashing
- [0009](./0009-sliding-and-session-windows.md) - Sliding and session windows, with lateness and staleness
- [0010](./0010-cross-series-arithmetic.md) - Cross-series arithmetic
- [0011](./0011-relabel-and-enrich-records.md) - Relabel and enrich records (proposed)
- [0012](./0012-join-transform.md) - Join transform: align N series, optionally reduce
- [0013](./0013-documentation-site.md) - Web presence: landing and Vocs docs
- [0014](./0014-local-state-inspection.md) - Local state inspection over gRPC
- [0015](./0015-nats-jetstream-backend.md) - NATS JetStream backend: streams, acks, and workers
- [0016](./0016-worker-ownership-leases.md) - Worker ownership leases: fail fast on a duplicate index
- [0017](./0017-sink-delivery-policy.md) - Sink delivery: retry through an outage
- [0018](./0018-wasm-transform.md) - WASM transform: a bytes ABI, sandboxed, local modules
- [0019](./0019-wasm-module-sourcing.md) - WASM module sourcing: oci:// and file:// references
- [0020](./0020-wasm-component-model.md) - WASM transform: adopt the Component Model
