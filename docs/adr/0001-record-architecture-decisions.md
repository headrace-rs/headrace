# 0001. Record architecture decisions

- Status: Accepted
- Date: 2026-08-03

## Context

Headrace makes design choices (backend, data model, windowing) that are hard to infer from code
alone and expensive to revisit blindly. We want the reasoning captured next to the code,
versioned, and reviewable.

## Decision

We will record significant architectural decisions as ADRs in `docs/adr/`, using the Nygard
format. An ADR is added in the same PR as the change it justifies, or ahead of it.

## Consequences

- New contributors and agents can read *why*, not just *what*.
- Superseding a decision is explicit: a new ADR links back to the old one, so history stays legible.
- Small overhead per architectural change; none for routine work.
