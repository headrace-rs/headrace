# 0006. Call processing nodes "transforms"

- Status: Accepted
- Date: 2026-08-03

## Context

The nodes that reshape and aggregate records were initially called "operators". Vector calls them
"transforms" and Fluvio uses transform-oriented language. "operator" also collides with the
Kubernetes-operator sense we use for the control plane.

## Decision

We will call record-processing nodes "transforms" in the IR, code, and docs. "operator" is
reserved for its Kubernetes meaning.

## Consequences

- Aligns vocabulary with Vector/Fluvio and removes the operator/Operator ambiguity.
- Requires a rename across the IR, code, and docs (the next unit).
