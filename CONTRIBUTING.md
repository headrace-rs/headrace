# Contributing

Thanks for contributing to Headrace. These guidelines keep history clean and review fast.

## Ground rules

1. Commits follow [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
2. Keep commits small and self-contained so each can be reviewed on its own. Split large
   changes into incremental commits/PRs; never mix unrelated changes in one commit.
3. Every change ships with tests (unit, plus integration where it fits). Coverage should not drop.
4. CI must be green before review: formatting, clippy, and tests.
5. Address review feedback by amending the relevant commit and rebasing, not with follow-up
   `fix: typo` commits. Keep history linear.

## Commit messages

A Conventional Commits subject in the present tense, imperative voice (`feat: add sliding
windows`, not `feat: added sliding windows` or `feat: adds sliding windows`), then an imperative
body that explains *why* when it is not obvious, wrapped at ~72 columns, ASCII only (no em-dash,
no `--`).

AI-assisted commits disclose the assistant with an `Assisted-by:` trailer, following the kernel
[coding-assistants guidance](https://docs.kernel.org/process/coding-assistants.html). Do not use
`Co-Authored-By`.

```
feat(window): add sliding windows

Fixed windows drop late-arriving samples at the boundary; sliding windows
smooth that over the configured step.

Assisted-by: Claude:claude-opus-4-8
```

Agents have extra rules in [AGENTS.md](./AGENTS.md).

## Development

```shell
cargo build
cargo test --workspace --all-features
cargo fmt --all
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo llvm-cov --workspace --all-features --locked --summary-only   # coverage, matches CI
```

Run a pipeline:

```shell
cargo run -p headrace -- run examples/latency.yaml
```

Install [prek](https://github.com/j178/prek) and enable the git hooks once. They run typos
and rustfmt on commit, a Conventional Commits check on the message, and clippy on push:

```shell
brew install prek   # or: cargo install --locked prek
prek install
```

## Rust style

Follow the [Rust Style Guide](https://doc.rust-lang.org/nightly/style-guide/); `rustfmt` and
`clippy` are enforced in CI. Prefer idiomatic Rust. Keep comments concise and reserved for the
non-obvious; do not narrate the code. Optional: [typos](https://github.com/crate-ci/typos) for
spell checking.

## Design and architecture

The system design lives in [DESIGN.md](./DESIGN.md). Significant architectural choices are
recorded as [ADRs](./adr/); propose one before a change that alters the architecture.
Use [mermaid](https://mermaid.js.org/) for diagrams (GitHub renders it).
