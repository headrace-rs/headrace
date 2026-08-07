# 0014. Local state inspection over gRPC

- Status: Accepted
- Date: 2026-08-07

## Context

Stateful transforms - `window` (open windows and their running aggregates) and `join` (buckets
of aligned, partly-filled inputs) - hold live state that is invisible today. When a rollup looks
wrong the only recourse is reading the sink output and guessing what the node buffered. We want to
ask a running pipeline "what is node `w` holding right now?" without a restart, extra logging, or a
metrics round-trip that only exposes counters, not the state itself.

Two shapes were considered:

- **REST/JSON.** No codegen, `curl`-friendly. But the state is typed and nested, a later `Watch`
  wants server streaming, and we already terminate gRPC (OTLP) - a second ad-hoc HTTP surface is
  more to own, not less.
- **gRPC.** Typed request/response, first-class server streaming for `Watch`, and it reuses the
  tonic stack OTLP already pulls in. The cost is proto codegen.

The codegen cost has a clean answer: generate the stubs once at dev time with `tonic-build` and
**check them in**, together with a `file_descriptor_set` for reflection, so the normal `cargo build`
never runs `protoc`. That matches how Headrace already ships OTLP (pre-generated
`opentelemetry-proto`, no build-time `protoc`) and keeps the `scratch` image build protoc-free.

## Decision

Add a gRPC **`State`** service that reports the live state of stateful nodes.

### Service

`package headrace.v1;` (versioned, room to grow):

```proto
service State {
  rpc Get(GetRequest) returns (GetResponse);
  // rpc Watch(WatchRequest) returns (stream NodeState);  // added after Get
}

message GetRequest  { repeated string node = 1; }   // node ids; empty = all stateful nodes
message GetResponse { repeated NodeState nodes = 1; }

message NodeState {
  string id = 1;
  string kind = 2;                    // "window" | "join"
  repeated GroupState groups = 3;
}

message GroupState {
  map<string, string> labels = 1;     // group_by dimension -> stringified value
  uint64 window_start_nanos = 2;
  uint64 window_end_nanos = 3;
  optional double value = 4;          // window: current running aggregate
  map<string, double> inputs = 5;     // join: per-input values filled so far, keyed by input id
  uint64 samples = 6;                 // window: records folded into this group so far
}
```

`Get` first (window state, then join). `Watch` (server-streamed snapshots on flush/eviction) is a
follow-up once `Get` lands; the service and message types are shaped so adding it needs no breaking
change.

### Mechanism - snapshot through the node's own loop

Each stateful node already runs a `tokio::select!` that owns its state exclusively (`window.rs`:
the loop owns `win: Window`). State is private to its transform by design (ADR-0007), so there is
no shared structure to lock. Inspection reuses that ownership:

- Each stateful node gains one more `select!` branch reading an `mpsc::Receiver<Snapshot>`, where
  `Snapshot = oneshot::Sender<NodeSnapshot>`. On a query the node builds a point-in-time snapshot
  of its open windows / buckets and replies on the oneshot.
- Because the reply is produced *by the node's own loop*, it is consistent with in-flight
  processing - never a torn read of a half-updated map, and no `Arc<Mutex<..>>` on the hot path.
- The runtime builds a registry `HashMap<node_id, mpsc::Sender<Snapshot>>` as it spawns stateful
  nodes and hands it to the `State` server. `Get` fans the query out to the requested nodes,
  collects the snapshots, and maps them to proto. A node that has exited (channel closed) is
  simply absent from the response.

This is preferred over the alternatives: a shared `Arc<Mutex<State>>` per node (lock contention on
the hot path, and the snapshot could still tear against a concurrent fold) or a metrics-style
periodic push (couples state to the metrics cadence and can't answer an on-demand query).

### Surface and opt-in

- The server runs **inside `headrace run`** only when `--inspect-addr <addr>` is passed (off by
  default; an admin surface is opt-in, like `--metrics`). It binds a separate port from OTLP.
- A `headrace inspect <addr> [--node <id>]...` subcommand is the gRPC client: it calls `State.Get`
  and prints a table. It is the same binary acting as its own client.

### Codegen and crates

- The generated stubs live in their **own crate `headrace-proto`** (checked-in `state.v1.rs` plus
  the `file_descriptor_set`), so the codegen is isolated from hand-written code and any future
  service lands in the same place. It depends only on `tonic` + `prost`.
- A sibling `headrace-proto-gen` crate (`publish = false`) depends on `tonic-build` and, run via
  `cargo run -p headrace-proto-gen`, compiles `proto/state.proto` into `headrace-proto/src`.
  `protoc` is needed only to regenerate, never to build - so `tonic-build` and `protoc` stay out of
  the normal build graph, and the `scratch` image stays protoc-free.
- `headrace-core` gains an `inspect` feature (which, like `otlp`, pulls `tonic`) and depends on
  `headrace-proto` under it for the server side; the `headrace` binary enables the feature and is
  its own client. `prost` joins the workspace deps.

## Consequences

- First non-OTLP service we own end to end: we take on a proto, checked-in stubs, and a
  regeneration step (documented, `protoc` only at dev time). The build and the `scratch` image stay
  protoc-free.
- Inspection adds one `select!` arm and a `snapshot()` method per stateful node; no change to the
  aggregation hot path and no shared-state locking (keeps ADR-0007's private-state model intact).
- The registry is process-local, so `Get` reflects only the nodes on this worker. Under the scaled
  NATS backend a node's state is one partition's worth; a cluster-wide view is a later fan-out over
  workers, out of scope here.
- `--inspect-addr` is unauthenticated and exposes raw state, so it is opt-in and meant for a trusted
  admin network (localhost, a debug sidecar), not the public data path.
- Shipping the `file_descriptor_set` means `grpcurl`/reflection works against a running pipeline for
  free.
