---
title: Install
description: Get the headrace binary - from source, Docker, or a Helm chart.
showAskAi: false
---

# Install

Headrace is a single self-contained binary. Pick the path that matches where you run it:
from source for dev and the edge, the container image for anything else, or the Helm chart
for Kubernetes.

## From source

You need a [Rust toolchain](https://rustup.rs) (stable, 1.85+). Build the release binary:

```sh
git clone https://github.com/headrace-rs/headrace
cd headrace
cargo build --release -p headrace
```

The binary lands at `target/release/headrace`. Install it onto your `PATH`:

```sh
cargo install --path crates/headrace
```

## Docker

The published image is a static binary on `scratch` - no base image, no CA certificates,
runs as a non-root uid:

```sh
docker pull ghcr.io/headrace-rs/headrace:latest
```

Mount a pipeline and expose the OTLP receiver:

```sh
docker run --rm -p 4317:4317 \
  -v "$(pwd)/pipeline.yaml:/pipeline.yaml" \
  ghcr.io/headrace-rs/headrace run /pipeline.yaml
```

The entrypoint is the binary itself, so everything after the image name is passed
straight to `headrace` (`run`, `validate`, `--metrics otlp`, ...).

## Kubernetes

The chart ships beside the image on GHCR. Its default pipeline receives OTLP, rolls up a
60s average per service, and prints JSON rollups to stdout:

```sh
helm install headrace oci://ghcr.io/headrace-rs/charts/headrace
```

Supply your own pipeline through `values.yaml` - the `pipeline:` block is the IR verbatim,
mounted at `/etc/headrace/pipeline.yaml` and passed to `headrace run`:

```yaml
# values.yaml
pipeline:
  sources:
    - { type: otlp, id: in, listen: 0.0.0.0:4317 }
  transforms:
    - type: window
      id: rollup
      input: in
      size: 60s
      group_by: [service.name]
      aggregate: { op: avg, field: value }
  sinks:
    - { type: otlp, id: out, input: rollup, endpoint: http://collector:4317 }
```

```sh
helm install headrace oci://ghcr.io/headrace-rs/charts/headrace -f values.yaml
```

The in-process backend keeps window state local to the pod, so the chart runs a single
replica; scaling past one needs the partitioned backend (roadmap).

## Verify

```sh
headrace --help
headrace run examples/latency.yaml   # generator -> filter -> 5s window -> stdout
```

Next: [Getting started](/getting-started) runs the bundled example and walks the CLI.
