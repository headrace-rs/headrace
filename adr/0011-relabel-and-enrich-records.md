# 0011. Relabel and enrich records

- Status: Proposed
- Date: 2026-08-06

## Context

`map` (ADR-0010) rewrites a record's numeric `value`. It does not touch a record's *identity* -
its metric `name` or its attribute keys and labels. Users need that too: a windowed average of
`req.count` should be emitted as, say, `req.count.avg` so it does not collide with the source
series, and derived series often need labels renamed, added, or dropped. Setting attributes shades
into *enrichment* (attaching reference data), so the feature is broader than a pure rename.

This is deferred to v0.7. This ADR records the intent and the open questions now, so a design that
came up in passing is not lost.

## Decision

We will add a small, stateless transform that reshapes a record's identity - separate from `map`
(which owns `value`) and `filter` (which selects whole records). Candidate surface:

```yaml
- type: relabel                     # name TBD: relabel | rename | set | enrich
  id: r
  input: rollup
  name: "req.count.avg"             # rewrite the metric name
  rename: { http.route: route }     # rename attribute keys
  set: { unit: ms }                 # set / overwrite attributes
  drop: [pod]                       # drop attributes
```

## Open questions

Resolve before implementing (v0.7):

- **Scope and name.** One transform (name + rename + set + drop) or several? Is it `relabel`
  (Prometheus-familiar), `rename`, `set`, or `enrich`?
- **Selector.** Should it apply only to records matching a condition ("where ...")? If so, match on
  what - attributes, metric name, value - and what is the selector called (`match`)? This is
  adjacent to `filter`, which already selects whole records; prefer one shared matcher concept
  across `filter` and this transform over two divergent condition syntaxes.
- **Set vs enrich.** Static `set` values are trivial; pulling values from an external table (true
  enrichment, with I/O and caching) is a much larger feature that may belong in `wasm` or a
  dedicated lookup node instead.

## Consequences

- Stateless and per-record, like `filter` and `map`: no windows, backend, or state, so it is
  unblocked and could land earlier than v0.7 if prioritized.
- Deciding the selector question also settles whether `filter` and this transform share a matcher;
  choosing now avoids two condition grammars later.
