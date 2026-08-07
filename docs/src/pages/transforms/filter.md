---
title: filter
description: Forward records that match a key predicate; drop the rest.
showAskAi: false
---

# Filter

The `filter` transform is stateless and per-record. It forwards records where `key` exists -
optionally requiring `key == equals` - and drops the rest. Dropped records are counted on
`headrace.records.dropped`.

```yaml
transforms:
  - type: filter
    id: only_checkout
    input: gen
    key: service.name   # attribute (or resource attribute) that must be present
    equals: checkout    # optional; omit to keep any record that has `key`
```
