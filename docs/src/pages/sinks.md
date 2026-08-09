---
title: Sinks
description: Where records leave a pipeline - stdout and OTLP.
showAskAi: false
---

# Sinks

A sink is where records leave the pipeline. Every sink has an `id` and an `input` naming its
upstream node. A pipeline needs at least one.

## stdout

Writes each record to standard output - for local runs, debugging, and `kubectl logs`.

```yaml
sinks:
  - type: stdout
    id: out
    input: windowed
    format: json   # text | json (default: text)
```

- **`text`** - a compact, human-readable line per record.
- **`json`** - one JSON object per record (the [record model](/concepts#the-record)), ready to
  pipe into `jq` or a log collector.

Keep headrace's own logs off stdout when you parse it: logs go to stderr, and self-telemetry
in `--metrics stdout` mode interleaves with data, so use `--metrics otlp` for clean output.

## otlp

An OTLP/gRPC exporter - forwards records to any OpenTelemetry-compatible backend (a Collector,
or a vendor endpoint).

```yaml
sinks:
  - type: otlp
    id: out
    input: windowed
    endpoint: http://collector:4317   # required
```

This is the other half of the pre-aggregation story: window upstream, then emit the aggregates
over OTLP so your backend ingests signal instead of the raw firehose. `endpoint` is required;
it speaks plain gRPC, same transport as the `otlp` source.
