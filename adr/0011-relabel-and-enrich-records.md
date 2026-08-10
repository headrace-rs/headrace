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

We will add a small, stateless transform, **`enrich`**, that reshapes a record's identity -
separate from `map` (which owns `value`) and `filter` (which selects whole records). Candidate
surface:

```yaml
- type: enrich
  id: r
  input: rollup
  name: "req.count.avg"             # rewrite the metric name
  rename: { http.route: route }     # rename attribute keys
  set: { unit: ms }                 # set / overwrite attributes
  drop: [pod]                       # drop attributes
```

Resolved since first drafting:

- **Name: `enrich`.** Chosen over `relabel` and `remap`, both of which carry ecosystem baggage that
  overpromises this transform's scope: Prometheus `relabel` implies its regex action model
  (`keep`/`drop`/`replace`/`hashmod`), and Vector `remap` implies VRL, a whole embedded language.
  `enrich` names the intent (reshape and attach identity) without importing either model.
- **One transform, not several.** `name` + `rename` + `set` + `drop` live in one `enrich` node.
- **Shared matcher with `filter`.** `enrich` and `filter` will share one predicate concept rather
  than grow two condition grammars. This is the decision to settle *before* implementing either
  the `enrich` selector or a richer `filter`.

## Open questions

Resolve before implementing:

- **The shared matcher's shape.** `filter` today is `key` exists / `equals`. Define the shared
  predicate type both `filter` and `enrich`'s optional selector ("apply only where ...") reference:
  what it matches on (attribute, metric name, value), how it composes (single condition vs.
  and/or), and its name (`match` / `where`). Do this once, on the `filter` side, so `enrich` adopts
  it rather than inventing a parallel grammar.
- **Static `set` now; external enrichment later.** Static `set` values land with `enrich`. True
  enrichment - pulling values from an external table, **pull- or push-based**, with I/O and caching
  - is a wanted follow-on but a much larger feature; it belongs in `wasm` or a dedicated lookup
  node, not folded into this stateless transform. Track it as its own roadmap item.

## Consequences

- Stateless and per-record, like `filter` and `map`: no windows, backend, or state, so `enrich` is
  unblocked and could land earlier than v0.7 if prioritized.
- The shared matcher is now a prerequisite: settling it unblocks both a richer `filter` and
  `enrich`'s selector, and avoids two condition grammars later.
- External (pull/push) enrichment is explicitly out of `enrich`'s scope and tracked separately, so
  the stateless transform stays small and statically validatable.
