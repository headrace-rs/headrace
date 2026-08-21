# Headrace - Design

OTel-native, stateful stream processing. Point telemetry at it, define aggregations
declaratively, emit to any OTLP-compatible backend. A single binary that runs in-process for dev
and edge, or scaled on Kubernetes over a partitioned backend.

**Principles**

- A layer, not infrastructure. Headrace has no broker, storage engine, or controller of its own.
  It runs on infrastructure you may already be running (NATS or Kafka, Kubernetes), because those
  tools are good at their jobs and we would rather not reinvent them.
- OTLP at the edges, a fixed transform catalog in the middle, WASM as the escape hatch.
- One binary; the deployment topology is chosen at startup.

## Dataflow

OTLP is decoded to an internal `Record` at ingest and encoded back at egress. Everything between
the edges operates on `Record`; transforms never see the wire format.

```mermaid
flowchart LR
  in[OTLP in] -->|decode to Record| src[Source]
  subgraph record[in flight: Record only]
    src --> f[filter / map]
    f --> w["window<br/>group_by + aggregate"]
    w --> snk[Sink]
  end
  snk -->|encode to OTLP| out[OTLP out / remote-write]
```

Two representations, kept separate:

- **Record** - the data in flight: the OTel data model, flattened to what transforms touch
  (`crates/headrace-core/src/record.rs`). The columnar path (OTel-Arrow / OTAP) is an internal
  optimization on this representation; users never handle it.
- **IR** - the program that processes records: Headrace's own transform-DAG spec (`headrace-ir`),
  OTel-aware but not OTLP. It is a config language, in the spirit of a Vector topology or a Flink
  job graph, and it is what an authoring agent targets once the MCP server lands (v0.5).

Stateless transforms (`filter`, `map`) hold nothing. State lives only in windowing transforms,
keyed by `group_by`, which is what makes horizontal scaling work.

## Run modes

The same binary takes a role at startup.

Monolithic, for dev and edge:

```mermaid
flowchart LR
  m["headrace run pipeline.yaml"] --> d["in-process backend<br/>in-memory state<br/>no external deps"]
```

Scaled, on Kubernetes:

```mermaid
flowchart TB
  i["ingress (N, stateless)"] -->|publish keyed by group_key| b[(backend<br/>partitioned)]
  b -->|partition p| w0["worker 0<br/>state for p"]
  b -->|partition q| w1["worker 1<br/>state for q"]
  w0 --> o[OTLP out]
  w1 --> o
```

## Scaling and the backend

Stateful scaling means partitioning the keyspace so every record for a `group_key` reaches the
same worker, where its window state stays local. Headrace does not build that machinery:

- **Partition assignment and record routing** -> the backend.
- **Pod lifecycle** -> Kubernetes (Deployment / StatefulSet).
- **Autoscaling** -> KEDA on backend lag.

**Who runs what.** You run the backend (NATS or Kafka) and Kubernetes, and give Headrace their
endpoints and credentials. Headrace provisions the topology it needs inside that backend from the
pipeline IR, idempotently at startup: the streams, the partitioned subjects, and the durable
consumers. You do not hand-create subjects. For locked-down environments, an admin can pre-create
the streams and give Headrace permission only to bind to them.

**Two edges, not one.** The backend is the *internal* edge between nodes: it routes each group's
records to the worker that owns them. OTLP is the *external* edge, how telemetry gets in and
results get out. You feed data through OTLP, not through the backend.

The `Backend` trait is the boundary (`crates/headrace-core/src/backend.rs`):
`producer(id, key_spec) -> Box<dyn Producer>` and `consumer(id) -> Box<dyn Consumer>`. The runtime
derives each edge's `key_spec` (the downstream transform's `group_by`) from the graph, and the
producer keys every record by it; `Producer::send(rec)` publishes. In-process ignores the key
(single worker); the JetStream implementation hashes it into a partition subject.

**Backend choice: NATS JetStream** (embeddable and familiar), with Redpanda or Kafka as a fallback
if elastic rebalance becomes a hard requirement. JetStream partitioning works but is not automatic
the way Kafka consumer groups are:

```mermaid
flowchart LR
  ig[ingress] -->|"hash(group_key) % P"| pt["client-side partition"]
  pt --> s0[["hr.pipe.node.0"]]
  pt --> s1[["hr.pipe.node.1"]]
  s0 -->|durable pull| w0["worker-0"]
  s1 -->|durable pull| w1["worker-1"]
```

- Headrace hashes the group key client-side (`hash(key) % P`, FNV-1a) and publishes to the
  partition subject `hr.<pipeline>.<node>.<p>`; no server-side subject transform to configure
  (ADR-0015).
- One durable pull consumer per partition; a worker binds the partitions for its StatefulSet
  ordinal (static assignment: `partition % replicas == ordinal`).
- Trade-off versus Kafka: scaling P is a rolling operation, not seamless elastic rebalance.
  Acceptable for the first scaled cut (v0.4).

**Why not consistent hashing yet.** A ketama-style ring would move fewer keys when the worker count
changes, but the cost that actually hurts is moving a key's *window state*, not remapping the key.
Until state checkpointing lands (v0.5), any reassignment drops and rebuilds in-flight windows
regardless of the hash, so the ring buys little. When we revisit assignment, the models to weigh
are key-groups (Flink) or rendezvous hashing rather than a plain ring, because they bound state
migration. Static assignment stays for the first scaled cut (v0.4) (ADR-0008).

**State durability (through v0.4): none.** On worker loss, in-flight windows are dropped and rebuilt
from the next events (at-most-once for in-flight aggregates). Checkpointing window state to a
compacted changelog or PVC is v0.5, at which point workers become a StatefulSet. Exactly-once is
out of scope.

## Stateful semantics

What the windowing transforms keep, and how it stays correct under scale and failure.

**Keyed on.** State is keyed by `(transform_id, group_key, window)`, where `group_key` is the
`group_by` tuple. That same `group_key` is the backend partition key, so a group's records, and
therefore its state, always land on one worker.

**Time is event time.** Windows are placed by the record's own `ts_nanos` (OTel `TimeUnixNano`),
not wall clock. v0.1 triggers flushes on processing time (simple, but wrong under lag or replay).
v0.2 moves to **watermarks**: `watermark = max_event_time - allowed_lateness`; a window
`[start, end)` emits when the watermark passes `end`; records later than that but within the
lateness bound update the emitted window, and records beyond it drop or route to a side output.

**Window kinds.** Tumbling today. **Sliding** (overlapping windows, so a record lands in several)
lands with watermarks in v0.2; **session** (gap-based: windows merge while events keep arriving and
close after an idle gap) comes later, in v0.7. Both carry per-window **lateness** and state
**staleness** (a TTL that evicts idle keys so an unbounded keyspace does not grow without limit).
These are event-time features (ADR-0009).

**Metric temporality is a first-class ingest concern**, and it is what makes this telemetry rather
than generic streams. OTel metrics are delta or cumulative; aggregation must normalize to delta on
ingest (or track per-series cumulative baselines and handle counter resets), or windowed sums are
wrong.

**State representation is hybrid.** No single format fits everything:

- *Data plane* (records in flight): **columnar** (Arrow / OTAP) for vectorized decode and filter
  and the network fast path.
- *Aggregation state* (the accumulators): **row/struct per key**. `{count, sum, min, max}` is tiny
  and point-mutated; columnar buys nothing.
- *Quantiles* (p99 latency): **mergeable sketches** (DDSketch / t-digest), never raw retention.

Every aggregate is a **monoid** (partial + partial = total). That property is what makes
cross-partition rollups and changelog recovery correct, and it is guarded by a proptest
(`crates/headrace-core/tests/aggregate_props.rs`).

**State is private to a transform.** A transform cannot read another transform's state. State is
co-located with the transform's partition, so a reference across transforms partitioned on
different keys would force a per-record distributed lookup and break locality (ADR-0007). Combine
state with one of two disciplined mechanisms instead:

- **join** (v0.3): co-partition both inputs on the join key, so both sides' state lands on the
  same worker and stays local. This is the Flink and Kafka-Streams model.
- **broadcast state**: a small, read-mostly table (rules, config, reference data) replicated to
  every partition.

Large reference data belongs in an external lookup, outside the state model.

**Inspecting state.** Because every aggregate is a monoid, partial state is meaningful to read. The
plan is a local, read-only view of current accumulators per `(transform_id, group_key, window)`,
via a `/state` admin endpoint and a `headrace inspect` command (v0.3). In the scaled deployment the
compacted changelog is itself the queryable state: the current value of a key is a read over its
changelog, the Kafka-Streams interactive-queries / materialized-view model (v0.5). A SQL grammar
over that state is a possible later step.

**Persistence has two distinct roles**, both satisfiable by JetStream:

1. *Record routing* - the partitioned subject/stream that carries records between ingress and
   workers.
2. *State changelog* - every mutation also appended to a **compacted** stream keyed by the state
   key; on crash or rebalance the new partition owner replays it to rebuild state before resuming.
   When keyed state outgrows RAM, back it with **RocksDB** (spill to disk).

## IR

Declarative, closed over a fixed catalog. Nodes wire by `input` id reference; every output has one
consumer (fan-out is a later `tee`). Full JSON Schema: `headrace schema`.

| Node | Kind | Notes |
|---|---|---|
| source | `generator` | synthetic metrics (dev/test) |
| source | `stdin` | one JSON `Record` per line |
| source | `otlp` | OTLP/gRPC receiver (`otlp` feature) |
| transform | `filter` | keep where `key` exists / equals |
| transform | `window` | tumbling + sliding, event-time; `group_by` + `aggregate {count,sum,min,max,avg}`; `on_missing {skip,error}`. Session windows *next* |
| transform | `map` | rewrite `value` from a numeric expression |
| transform | `join` | cross-series arithmetic on aligned windows |
| transform | `wasm` | run a sandboxed WebAssembly module per record (`wasm` feature); module from a path, `file://`, or digest-pinned `oci://` (`wasm-oci`) |
| sink | `stdout` | text / json |
| sink | `otlp` | OTLP/gRPC exporter (`otlp` feature); Prometheus remote-write *next* |

```yaml
sources:  [{ type: generator, id: gen, interval: 200ms }]
transforms:
  - { type: filter, id: only_checkout, input: gen, key: service.name, equals: checkout }
  - type: window
    id: rollup
    input: only_checkout
    size: 5s
    group_by: [service.name, http.route]
    aggregate: { op: avg, field: value }
sinks:    [{ type: stdout, id: out, input: rollup, format: text }]
```

## Pipeline lifecycle and control plane

Where a pipeline definition lives, and how you create or update one, without building a REST API.

- **Now (until the CRD, v0.6):** the IR is a file (`headrace run f.yaml`) or a ConfigMap on
  Kubernetes. GitOps (Argo/Flux) is the create/update path.
- **v0.6:** a **`Pipeline` CRD whose `spec` is the IR verbatim**; `status` reports observed state
  (running, per-node lag, assigned partitions, errors). A thin operator reconciles `Pipeline` CRs
  into Deployments and backend subjects. The CRD's OpenAPI v3 validation schema is generated from
  the IR JSON Schema (`headrace schema`), so `kubectl apply` validates for free and you inherit
  RBAC, GitOps, and admission webhooks.
- **Authoring API:** the v0.5 MCP server is how an agent creates a dataflow: emit IR, validate
  against the schema, dry-run, then write it as a file or CR. No custom REST surface.

This control-plane operator (CR -> Deployment) is separate from the *no data-plane controller*
stance above: partition assignment stays the backend's job, and the operator only manages pipeline
lifecycle. Runtime aggregation state never lives in the CR; that is the changelog or PVC (see
*Stateful semantics*).

## Internal record model

`crates/headrace-core/src/record.rs` - the OTel data model, flattened to what nodes touch:

```
Record { ts_nanos, start_ts_nanos: Option, resource: Attrs, scope, name, value: f64, attrs: Attrs }
Attrs   = map<string, AttrValue{ bool | int | double | str }>   # OTel AnyValue subset
```

Window rollups set `start_ts_nanos`/`ts_nanos` to the window `[start, end)` (OTel
`StartTimeUnixNano`/`TimeUnixNano`); point samples leave `start_ts_nanos` unset.

v0.1 is metrics-shaped (`value: f64`). Logs and traces widen `value` to an enum; the attribute
model is already OTel-compatible.

## Self-telemetry

Headrace records its own metrics through a `Metrics` boundary (`headrace-core::metrics`), a no-op by
default so the core carries no OpenTelemetry dependency. The `headrace` binary supplies an
OTel-backed recorder (`--metrics stdout|otlp`, off by default) that exports `headrace.records.out`,
`headrace.records.dropped` (labeled by `reason`: filtered / invalid / late / incomplete / capped),
`headrace.window.flushes` / `.groups`, and `headrace.node.errors`, attributed by node id and kind. The instruments are the one piece of state
shared across node tasks, handed out as cheap `Arc` clones. Headrace emits over OTLP, the same
protocol it ingests.

## Prior art

Headrace takes inspiration from tools we like and use:

- **Vector** - the config ergonomics and transform-topology shape, and observability-first edges.
- **Fluvio SDF** - stateful dataflow with WASM transforms and keyed, materializable state; the
  closest analog to what Headrace does.
- **Flink** - the reference for event-time processing, watermarks, and stateful scaling.
- **Kafka Streams** - changelog-based state recovery and the co-partitioned join model.
- **Arroyo** - Rust and Arrow streaming with event-time semantics.

Headrace's own bet is narrow: OTel-native (OTLP edges, metric temporality handled on ingest), a
single binary, and renting the backend rather than shipping one.

## Crate layout

```mermaid
flowchart TD
  cli["headrace (bin)<br/>run / validate / schema / inspect"] --> core[headrace-core]
  cli --> ir[headrace-ir]
  cli --> proto[headrace-proto]
  core --> ir
  core --> record[headrace-record]
  core -.inspect.-> proto
```

- `headrace-ir` - IR types + JSON Schema. No runtime deps.
- `headrace-record` - the record data model (`Record`, `AttrValue`), shared by the engine and the wasm guest SDK so the two cannot drift; also holds the wasm `ABI_VERSION`. No runtime deps.
- `headrace-core` - `Backend` trait (in-process default), transforms, runtime, and the `Metrics` boundary. Optional features: `otlp` (source/sink), `nats` (JetStream backend), `inspect` (state-inspection gRPC server), `wasm` (WebAssembly transform).
- `headrace-proto` - checked-in gRPC stubs for the state-inspection API, used by the `inspect` feature and the `inspect` CLI command.
- `headrace-proto-gen` - dev-only tool that regenerates `headrace-proto` from the `.proto`; not built at runtime.
- `headrace-wasm-guest` / `headrace-wasm-macro` - author-facing SDK for writing a `wasm` transform in Rust (an `#[transform]` fn) and its proc-macro. Compiled to wasm32 by module authors; not linked into the binary.
- `headrace` - CLI + the OTel self-metrics exporter (the only crate that depends on OpenTelemetry).

## Testing

Unit and property tests live with the code (`#[cfg(test)]` and `tests/`); time-dependent behavior
is driven by the paused tokio clock, never wall-clock sleeps. The whole OTLP path has end-to-end
coverage as a **cargo integration test** (`crates/headrace-core/tests/otlp_e2e.rs`), no external
services, run by `cargo test --all-features`:

1. Stand up Headrace's OTLP receiver on an ephemeral port, feeding a `window` rollup.
2. Push a known series over gRPC, so the expected rollup is deterministic.
3. Headrace's OTLP sink exports to a mock OTLP receiver stood up inside the test (a tonic server
   that records requests).
4. Assert the aggregated value, labels, and window bounds on what the mock received.

A heavier variant behind a `docker` feature swaps the mock receiver for a real `otelcol` via
testcontainers, to prove wire-compatibility with the Collector.

## Packaging and distribution

A **Helm chart** (v0.2) under `charts/headrace` deploys what v0.1+OTLP is: the `headrace`
binary as a Deployment, a Service for the OTLP receiver (gRPC 4317 / HTTP 4318), and the pipeline IR
mounted from a ConfigMap. Values toggle the backend (`backend: inprocess|nats`) and, later, a KEDA
`ScaledObject`; `appVersion` tracks the crate version.

Published to **GHCR** as an OCI artifact - GitHub Container Registry (`ghcr.io`), which is part of
GitHub, not a Google/GCP service. CI packages the chart and `helm push`es it; users install with:

```sh
helm install headrace oci://ghcr.io/headrace-rs/charts/headrace
```

No separate chart repo and no third-party account: the chart sits beside the container images,
pushed with the Actions `GITHUB_TOKEN`. (The classic `helm/chart-releaser-action` publishing an
`index.yaml` to GitHub Pages stays a fallback.) The near-term goal is to drop this into a real
cluster, point an OTLP exporter at it, run an aggregation, and sink to a collector - the OTLP
round-trip test above, but live.

The v0.2 deployable unit is a single stateless-to-configure binary: no external backend, IR from a
ConfigMap, OTLP in and OTLP out.

```mermaid
flowchart LR
  exp["OTLP exporter(s)"] -->|"gRPC :4317"| svc["Service"]
  subgraph k8s["Kubernetes"]
    svc --> pod["headrace pod<br/>in-process backend"]
    cm["ConfigMap<br/>pipeline IR"] -.mounted.-> pod
  end
  pod -->|OTLP out| col["downstream collector"]
```

## Roadmap

The bet is to prove core processing correctness on the in-process backend, deployable and testable
in a real cluster, *before* taking on a distributed backend. Scale-out (NATS), extensibility
(WASM), durability, and the control plane follow once the core is proven.

```mermaid
flowchart LR
  v02["v0.2 · deployable core<br/>OTLP · Helm/GHCR<br/>event-time + watermarks<br/>sliding windows"]
  v03["v0.3 · richer transforms<br/>map · join<br/>state inspection"]
  v04["v0.4 · scale + extend<br/>NATS backend<br/>WASM · docs"]
  v05["v0.5 · durable + authoring<br/>checkpointing<br/>interactive queries<br/>MCP server"]
  v06["v0.6 · control plane<br/>Pipeline CRD + operator"]
  v07["v0.7 · session windows"]
  v02 --> v03 --> v04 --> v05 --> v06 --> v07
```

**v0.1 (done)** - IR with static validation (refs, cycles, durations), in-process backend,
generator/stdin -> filter/window -> stdout, task supervision (fail fast on node error or panic),
graceful drain on SIGINT/SIGTERM (a second signal forces), OTel self-metrics.

**v0.2 - deployable core processing** (in-process backend, no external dependency), in sequence:

1. **OTLP source/sink** *(done)* - ingest and emit real OTLP; decode to `Record` with
   cumulative-to-delta normalization on ingest, encode back on egress.
2. **OTLP round-trip integration test** *(done)* - locks the wire contract (see *Testing*).
3. **Helm chart + GHCR** *(done)* (see *Packaging and distribution*) - drop the single binary
   into a real cluster, point an OTLP exporter at it, and sink rollups to a collector.
4. **Event-time + watermarks** *(done)* - place windows by the record's own time and close them
   on a watermark (`max_event_time - allowed_lateness`), replacing processing-time flushes.
5. **Sliding windows** *(done)* - overlapping windows (a record lands in several). Per-key state
   staleness (a TTL that evicts idle keys) is a follow-up (ADR-0009).

Branding and logo: done.

**v0.3 - richer transforms and introspection** (still in-process), in sequence:

1. **`map` + `join`** *(done)* - an expression transform (closed numeric expression over
   `value` and fields) plus a co-partitioned join; together they unlock cross-series arithmetic
   like `a - b` (ADR-0010, ADR-0012). Join aligns windowed inputs on their shared `group_by` and
   window, and optionally reduces them with a `value` expression.
2. **Local state inspection** *(done)* - a read-only view of each stateful node's open windows
   and join buckets, via a `State` gRPC service (`Get` plus a streaming `Watch`) served on
   `run --inspect-addr`, with a `headrace inspect` client (ADR-0014). Snapshots are pulled
   through each node's own loop, so they never lock or tear against processing.

**v0.4 - scale-out and extensibility**, in sequence:

1. **NATS JetStream backend** *(done)* - the scaled path: partitioned subjects, durable consumers,
   static assignment (ADR-0008, ADR-0015). The first deployment that needs an external backend.
2. **WASM transform** *(done)* - the escape hatch for custom logic: a sandboxed module runs per
   record over a MessagePack bytes ABI, authored with the `headrace-wasm-guest` SDK (ADR-0018).
3. **Docs site** *(done)* (Vocs, mermaid) on Cloudflare Pages.
4. **Transport security (TLS / mTLS)** - optional TLS on the OTLP receiver and the state
   inspection endpoint, and mTLS on the internal backend edge. Until then those surfaces rely
   on network placement (SECURITY.md) plus the receiver's resource caps (`max_recv_bytes`,
   `max_concurrent_streams`); TLS is what makes them safe to cross a trust boundary. Scale-out
   is the point this starts to matter, since it is the first topology with network hops.

**v0.5 - durability and authoring:**

1. **State checkpointing** - mutations to a compacted changelog, replayed on rebalance, with
   RocksDB spill when state outgrows RAM. Workers become a StatefulSet; at-most-once becomes
   durable. Depends on the v0.4 backend.
2. **Interactive state queries** - cluster-wide reads over the changelog (the Kafka-Streams
   materialized-view model). Depends on the changelog from the previous step.
3. **MCP authoring server** - an agent emits IR, validates against the schema, dry-runs, then
   writes it as a file or CR. No custom REST surface.

**v0.6 - control plane:**

- **`Pipeline` CRD + operator** - `spec` is the IR verbatim, `status` reports observed state; a thin
  operator reconciles CRs into Deployments and backend subjects. The lifecycle layer.

**v0.7 - session windows** - gap-based windows that merge while events keep arriving and close after
an idle gap. Event-time (watermark-driven), so it builds on the v0.2 foundation.
