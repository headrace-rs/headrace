---
title: Troubleshooting
description: Late and dropped records, quiet streams, and validation errors.
showAskAi: false
---

# Troubleshooting

Most surprises show up in one of two places: [self-metrics](/reference/metrics) while a
pipeline runs, or `headrace validate` before it does.

## Late records

**Symptom:** `headrace.records.dropped{reason=late}` climbs.

A record whose window has already fired is dropped and counted here. The cause is that
`allowed_lateness` is smaller than the source's real out-of-orderness, so windows fire before
the late records arrive.

**Fix:** raise `allowed_lateness` on the window. It trades latency for completeness - a larger
grace fires later but folds in more late data. A steady nonzero rate means the current value is
too small; zero means you may be waiting longer than you need to.

## Dropped records

**Symptom:** `headrace.records.dropped` climbs on a `window` or `map` node.

A referenced field could not be read as a number and the policy is `skip` (the default). Two
distinct cases, with distinct policies:

- **`on_missing`** - the field is absent from the record.
- **`on_invalid`** - the field is present but non-numeric, or the result is non-finite (e.g.
  divide by zero).

**Fix:** confirm the field name and where it lives - attributes fall back to resource
attributes, and `value` (or an omitted field) means the record's own value (see the
[record model](/concepts#the-record)). To fail loud instead of silently skipping, set the
policy to `error`.

## Windows never fire

**Symptom:** a stream goes quiet and its last windows never emit.

The watermark only advances when records arrive, so a window with no newer events stays open.

**Fix:** set `idle_timeout` on the window to force-close open windows after that much
wall-clock silence. A clean shutdown (Ctrl-C) also flushes open windows.

## Unexpected group cardinality

**Symptom:** `headrace.window.groups` is far larger than expected, or memory grows.

Each distinct combination of `group_by` values is a live group held in state. A high-cardinality
key (a raw id, a URL with parameters) explodes the group count.

**Fix:** group by bounded labels (`service.name`, a normalized `http.route`), not unbounded
ones. Use a `map` or `filter` upstream to normalize or drop the offending dimension.

## Validation errors

`headrace validate` rejects a pipeline before it runs. What it checks, and the usual cause:

| Error | Cause |
|---|---|
| duplicate id | two nodes share an `id`; ids must be unique across the whole pipeline. |
| unknown input | an `input` names a node that does not exist - check for a typo. |
| multiple consumers | an output feeds more than one downstream node. Fan-out is not allowed; only `join` fans in. |
| unreachable | a transform no source can reach (an orphan, or a cycle). |
| invalid window | `slide` is zero, or longer than `size` (records would fall between windows). |
| bad expression | a `map` or `join` `value` expression is malformed; see the [grammar](/transforms/map#expression-grammar). |
| join not aligned | a `join`'s inputs are not windows, or disagree on `group_by` or window size. |
| align-only join to a sink | a `join` with no `value` carries attributes for a downstream node, so it cannot feed a sink. |

Run it in CI - it is fast, needs no backend, and turns a runtime failure into a build failure:

```sh
headrace validate pipeline.yaml
```
