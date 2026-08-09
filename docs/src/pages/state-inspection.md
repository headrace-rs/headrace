---
title: State inspection
description: Read a running pipeline's live window and join state over gRPC.
showAskAi: false
---

# State inspection

Stateful transforms hold data in flight - a `window` keeps its open windows and their running
aggregates; a `join` keeps buckets of partly-aligned inputs. State inspection lets you ask a
**running** pipeline what a node is holding right now, without a restart, extra logging, or
waiting for the value to reach a sink.

It is a read-only gRPC service (`State`) that Headrace serves on demand, plus a built-in client
(`headrace inspect`).

## Enable it

The server is **off by default**. Start `run` with `--inspect-addr` to open it on its own port:

```sh
headrace run pipeline.yaml --inspect-addr 127.0.0.1:4318
```

It exposes raw node state and is unauthenticated, so bind it to a trusted network only -
localhost, or a debug sidecar - never the public data path.

## Query it

From another shell, point `headrace inspect` at that address:

```sh
headrace inspect 127.0.0.1:4318
```

```
hi (window) - 3 group(s)
  service.name=checkout  window=[1786089380000000000,1786089385000000000)  value=92  samples=2
  service.name=cart      window=[1786089380000000000,1786089385000000000)  value=99  samples=2
  service.name=search    window=[1786089380000000000,1786089385000000000)  value=85  samples=2
spread (join) - 1 group(s)
  service.name=checkout  window=[1786089385000000000,1786089390000000000)  inputs={hi=250}  samples=1
```

Restrict to specific nodes with `--node` (repeatable); omit it for every stateful node:

```sh
headrace inspect 127.0.0.1:4318 --node hi --node spread
```

## What you see

Each stateful node reports its **open** groups - the ones that have not yet fired or been
evicted. Fired windows and completed join buckets have already left as records, so they are not
in the snapshot.

- **`window`** - one line per open window and group: the `group_by` labels, the window bounds
  `[start, end)` in epoch nanoseconds, the group's current running `value` (the aggregate so
  far), and `samples`, how many records have folded into it. A window climbs in `samples` until
  the watermark fires it (see [Windowing](/transforms/window)).
- **`join`** - one line per open bucket: the shared labels, the window bounds, and `inputs`, the
  per-input values that have arrived so far keyed by input id (e.g. `inputs={hi=250}` means only
  `hi` has landed). `samples` is how many of the inputs have arrived. A bucket has no single
  `value` until every input arrives and it fires (see [Join](/transforms/join)).

A snapshot is produced by the node's own task as it answers the query, so it is always
consistent with the records the node has already processed - never a torn read.

## The gRPC API

The service is `headrace.v1.State` with a unary `Get`:

```proto
service State {
  rpc Get(GetRequest) returns (GetResponse);   // GetRequest { repeated string node }
}
```

Headrace serves gRPC reflection from the bundled schema, so tools like `grpcurl` work against a
running pipeline without a local `.proto`:

```sh
grpcurl -plaintext 127.0.0.1:4318 headrace.v1.State/Get
grpcurl -plaintext -d '{"node":["hi"]}' 127.0.0.1:4318 headrace.v1.State/Get
```

## Scope

A snapshot reflects only the nodes on the worker you connect to. On the in-process backend
that is the whole pipeline. On a partitioned backend each worker holds one partition's share of
a node's state, so a cluster-wide view is a fan-out over workers - out of scope here.
