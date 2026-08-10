# headrace

[![crates.io](https://img.shields.io/crates/v/headrace?style=flat-square)](https://crates.io/crates/headrace)
[![docs](https://img.shields.io/badge/docs-headrace.rs-e76125?style=flat-square)](https://headrace.rs/docs)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=flat-square)](https://github.com/headrace-rs/headrace/blob/main/LICENSE)

OTel-native, stateful stream processing in a single Rust binary. Point OpenTelemetry at it,
declare aggregations in YAML, and forward the aggregated metrics to any backend - not every
datapoint.

## Install

```sh
cargo install headrace                     # from crates.io
cargo binstall headrace                    # prebuilt binary, no compile
docker pull ghcr.io/headrace-rs/headrace   # container image
```

Prebuilt binaries for Linux and macOS are also attached to each
[release](https://github.com/headrace-rs/headrace/releases).

## Use

```sh
headrace run pipeline.yaml        # run until Ctrl-C / SIGTERM
headrace validate pipeline.yaml   # parse and statically check
headrace schema                   # print the IR JSON Schema
```

A minimal pipeline - average a metric over 5s windows per service, printed to stdout:

```yaml
sources:
  - { type: generator, id: gen, interval: 200ms }
transforms:
  - type: window
    id: rollup
    input: gen
    size: 5s
    group_by: [service.name]
    aggregate: { op: avg, field: value }
sinks:
  - { type: stdout, id: out, input: rollup, format: text }
```

It also exports Headrace's own metrics over OTLP (`--metrics stdout|otlp`).

Full docs at [headrace.rs/docs](https://headrace.rs/docs); source and design at
[github.com/headrace-rs/headrace](https://github.com/headrace-rs/headrace).

## License

Apache-2.0.
