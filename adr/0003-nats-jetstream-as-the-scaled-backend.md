# 0003. NATS JetStream as the scaled backend

- Status: Accepted
- Date: 2026-08-03

## Context

Scaling stateful transforms means partitioning the key space so a group's records and state land
on one worker (ADR-0005). That needs a backend that can partition and durably transport between
ingress and workers. Kafka/Redpanda offer consumer-group rebalancing; NATS JetStream is lighter,
embeddable, and already familiar to us.

## Decision

We will target NATS JetStream as the scaled backend: partition via a server-side subject
transform with one durable pull consumer per partition, and bind workers to partitions by
StatefulSet ordinal. Redpanda/Kafka remain a fallback only if seamless elastic rebalance becomes
a hard requirement.

## Consequences

- Lighter operational footprint and one fewer heavy dependency for typical deployments.
- Scaling partition count is a rolling operation, not seamless elastic rebalance; acceptable for now.
- The `Backend` trait keeps this swappable; the in-process backend stays the default for dev/edge.
