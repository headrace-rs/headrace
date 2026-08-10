---
title: Sources
description: Where records enter a pipeline - OTLP, stdin, and the generator.
showAskAi: false
---

# Sources

A source is where records enter the pipeline. Every source has an `id`; downstream nodes name
it as their `input`. A pipeline needs at least one.

## otlp

An OTLP/gRPC receiver - the primary way real telemetry gets in. Point any OpenTelemetry SDK or
Collector at it.

```yaml
sources:
  - type: otlp
    id: in
    listen: 0.0.0.0:4317          # bind address (default: 0.0.0.0:4317)
    max_recv_bytes: 4194304        # reject requests larger than this (default: 4 MiB)
    max_concurrent_streams: 256    # per-connection HTTP/2 stream cap (default: 256)
```

Incoming OTLP metrics are normalized into the internal [record](/concepts#the-record) model,
including cumulative-to-delta conversion where needed. It speaks plain gRPC; no TLS or auth is
terminated in-process, so front it with a trusted network or a sidecar if you need those. The
receiver is an untrusted-input boundary, so `max_recv_bytes` and `max_concurrent_streams` cap
the memory and stream fan-in a single client can force; both have safe defaults and rarely need
tuning. Transport TLS/mTLS is on the roadmap.

## stdin

Reads one JSON-encoded record per line from standard input - handy for tests, replay, and
piping fixtures:

```yaml
sources:
  - { type: stdin, id: in }
```

```sh
echo '{"name":"req.latency","value":42,"ts_nanos":0,"attrs":{"http.route":"/checkout"}}' \
  | headrace run pipeline.yaml
```

Each line must be a full record; `name`, `value`, and `ts_nanos` are required, the rest
default (see the [record model](/concepts#the-record)).

## generator

Synthetic metrics for demos and tests - no external input needed. It emits a metric on an
interval, spread across the services and routes you list.

```yaml
sources:
  - type: generator
    id: gen
    metric: demo.metric        # metric name (default: demo.metric)
    interval: 500ms            # time between samples (default: 500ms)
    services: [checkout, cart] # service.name values to spread across (default: none)
    routes: [/checkout, /cart] # http.route values to spread across (default: none)
```

The bundled `examples/latency.yaml` uses the generator, so `headrace run examples/latency.yaml`
works with no collector attached.
