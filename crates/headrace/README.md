# headrace

The Headrace CLI and binary.

```sh
headrace run pipeline.yaml        # run a pipeline until Ctrl-C / SIGTERM
headrace validate pipeline.yaml   # parse and statically check
headrace schema                   # print the IR JSON Schema
```

It also exports Headrace's own metrics via OpenTelemetry (`--metrics stdout|otlp`); this is
the only crate that depends on OpenTelemetry. See [DESIGN.md](../../DESIGN.md).
