# 0017. Sink delivery: retry through an outage

- Status: Accepted
- Date: 2026-08-16

## Context

A sink is the pipeline's egress - the OTLP exporter today, Prometheus remote-write later. The
`stdout` sink is local and cannot fail. The OTLP exporter connects at startup and `export`s
batches, and today a connect failure or any export error is **fatal**: the node errors and the
whole pipeline stops. The only resilience is backpressure for a *slow* sink (the bounded per-edge
channel). So a transient collector outage - a restart, a network blip - takes the pipeline down,
even though the NATS producer already retries through such outages with backoff (ADR-0015).
This closes that asymmetry on the egress path.

## Decision

A sink will retry through an outage rather than fail, mirroring the NATS producer.

- **Retry with capped exponential backoff.** A failed `export`, and the startup connect, retry
  indefinitely with the backoff the NATS backend uses (100ms -> 5s). The startup connect no longer
  fails fast when the endpoint is not up yet.
- **Backpressure, not a buffer.** While retrying, the sink stops draining its input, so the bounded
  per-edge channel fills and back-pressures upstream to the source. The channel is the bound; no
  separate buffer. A long or permanent outage stalls the pipeline rather than dropping - the
  at-least-once choice (stall over loss), consistent with ADR-0015.
- **All export errors are transient** for now. OTLP metric export rarely fails permanently (a
  collector accepts most payloads; failures are `unavailable`-class). A dead-letter destination and
  transient/permanent classification are deferred until a concrete poison case appears.
- **Per-sink config** (backoff bounds) defaults to the above. A retry logs a warning, so a stalled
  sink is visible.

Recovery is automatic: when the sink returns, the retry succeeds, the batch flushes, backpressure
releases, and ingestion resumes. Durable recovery across a worker *crash* is out of scope - in-
process state is in memory, so a crash loses the in-flight batch; durability is the NATS path plus
checkpointing (v0.5).

## Consequences

- A transient sink outage no longer kills the pipeline; it stalls and resumes. At-least-once egress
  means a retried export can duplicate on an uncertain ack, which we accept (as in ADR-0015).
- A permanently-down sink stalls the pipeline back to the source. Intended (no silent loss), but it
  couples ingestion to sink health, so operators watch for the stall.
- No DLQ yet, so a poison batch, should one occur, blocks its sink until fixed. Revisit if it bites.
- Retry + backoff + backpressure now appears in both the NATS producer and the sink - worth a shared
  helper if a third user shows up.
