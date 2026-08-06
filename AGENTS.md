# AGENTS.md

Guidance for AI coding agents in this repo. Human contributors: see [CONTRIBUTING.md](./CONTRIBUTING.md).

## Workflow

- Clarify the design before implementing. For anything non-trivial, agree on the approach first
  and diagram it; prefer a short design note or an [ADR](./docs/adr/) over jumping to code.
- One unit of change per commit. Never mix unrelated changes. Present the change for review
  before committing.
- Every change ships with tests. Run local CI before calling it done, and do not claim it passes
  without running it.
- Verify against the code and the tools: read before you answer, run before you assert.

Local CI:

```shell
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features
```

## Writing: code, comments, docs, commits

- Concise and to the point. No fluff. Explain the non-obvious; do not narrate the obvious.
- ASCII only. No em-dash and no `--`; write `-`. Use `->` not the arrow glyph, `!=` not the
  not-equal glyph, and so on.
- Comments justify *why*, not *what*. Delete any comment that restates the code.
- Use [mermaid](https://mermaid.js.org/) for designs worth a picture.
- Do not use the word "seam"; say boundary, interface, or extension point.
- Do not use "bespoke"; say "custom".

## Commits

- Conventional Commits (see CONTRIBUTING.md). Write the subject in the present tense, imperative
  voice: `feat: add sliding windows`, not `added` or `adds`.
- Disclose AI with an `Assisted-by: Claude:claude-opus-4-8` trailer. Never `Co-Authored-By`, and
  never add a human's `Signed-off-by`.

## Tests

- Unit tests inline (`#[cfg(test)] mod tests`); public-surface and cross-crate tests in `tests/`.
- Put helpers *after* the tests that use them.
- Prefer deterministic time: drive tokio's paused clock, not sleeps or `yield_now` loops.
- Coverage: never land a change that lowers workspace coverage below its current level
  (about 90% line / 87% region). New code ships with tests that hold or raise it. Measure
  with `cargo llvm-cov --workspace --all-features --summary-only`.

## Terminology

- **node**: a vertex in the pipeline DAG (source, transform, or sink), run as one async task.
- **transform**: a node that reshapes or aggregates records (filter, window, ...). Say
  "transform", not "operator".
- **worker**: a process/pod running one keyspace partition in the scaled deployment.
- Headrace is general-purpose telemetry and stream processing.

## Code conventions

- Import a type by one path and use it consistently (e.g. a single `use std::sync::Arc`, not
  mixed inline `std::sync::Arc` paths).
- Prefer `tokio::fs` in async paths unless `std::fs` is clearly fine (small, at startup, no
  blocking concern).
- Document public items with rustdoc; keep it accurate and free of drift.

## CI workflows

- GitHub Actions live in `.github/workflows`. Write the workflow `name:`, every job name, and
  every named step in Sentence case, matching `ci.yml` (e.g. `name: CI`, `Check formatting`).
- Keep workflows minimal and scoped to one purpose; prefer the built-in `GITHUB_TOKEN` over a
  personal access token.

## Architecture

- Record architectural decisions as [ADRs](./docs/adr/); propose one before changing the
  architecture.
- The system design lives in [DESIGN.md](./DESIGN.md); keep it in sync.
