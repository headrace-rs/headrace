# 0002. Headrace is a layer, not infrastructure

- Status: Accepted
- Date: 2026-08-03

## Context

The streaming-infrastructure space (Kafka, Redpanda, NATS, Pulsar) is mature and heavily
contested. Rebuilding a broker, storage engine, or partition controller is enormous, low-moat
work, and it contributed to earlier projects in this space stalling.

## Decision

We will build Headrace as a processing layer on top of existing transport, not as
infrastructure. We rent the broker, the storage, and partition assignment; we do not build a
broker, a storage engine, or a custom data-plane controller. Our value is OTLP-native, stateful
transforms and the authoring experience.

## Consequences

- Far smaller surface to maintain; effort goes into transforms, correctness, and UX.
- Horizontal scaling leans on the backend's partitioning and on Kubernetes for lifecycle (ADR-0003).
- We depend on a backend's semantics rather than controlling them end to end.
- A control-plane operator that reconciles pipeline definitions is still in scope; that is not a
  data-plane controller.
