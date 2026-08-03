# 0004. OTLP at the boundaries, uniform Record model

- Status: Accepted
- Date: 2026-08-03

## Context

Headrace is OTel-native. OTLP is the wire format at ingress and egress, but the protobuf types
are awkward to process directly, and transforms should not each re-parse OTLP.

## Decision

We will decode OTLP to one internal `Record` at ingress and re-encode at egress. Transforms only
see `Record`. A columnar fast path (Arrow / OTel-Arrow) is an internal optimization behind that
boundary, invisible to transforms and users.

## Consequences

- Transforms are simple and uniform (`Record -> Record`); there is no per-transform input schema.
- The IR (`headrace-ir`) describes transform configuration; the data shape is `Record`.
- v0.1 `Record` is metrics-shaped (`value: f64`); logs and traces widen it later without changing
  the boundary contract.
