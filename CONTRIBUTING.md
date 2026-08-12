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

### Running against NATS (scaled backend)

The default backend is in-process. To use the NATS JetStream backend (ADR-0015) you need a
JetStream-enabled NATS server. Start one with Docker:

```shell
docker run --rm -p 4222:4222 nats:2.10 -js
```

or the standalone binary (`brew install nats-server`):

```shell
nats-server -js
```

Then run a pipeline over it - the `headrace` binary always includes the NATS backend, so no
extra feature flag is needed:

```shell
cargo run -p headrace -- run examples/latency.yaml \
  --backend nats --nats-url nats://127.0.0.1:4222
```

Headrace provisions a work-queue stream per node output on startup, with subjects
`hr.<name>.<node>.<partition>` (`<name>` defaults to the pipeline file stem or is set with
`--name`). Inspect them with the [`nats`](https://github.com/nats-io/natscli) CLI:

```shell
nats stream ls        # the hr_<name>_<node> streams
nats stream report
```

To scale out, split each edge into `--partitions` partitions (default 12) and run `--workers`
copies, each with a distinct `--worker-index` in `0..workers` (or set `HEADRACE_WORKER_INDEX`,
e.g. from a StatefulSet ordinal). A record routes to `hash(key) % partitions`, and worker `i`
owns the partitions where `p % workers == i`, so all state for a key stays on one worker.
Point every worker at the same pipeline and `--name` so they share the streams:

```shell
# terminal 1
cargo run -p headrace -- run examples/latency.yaml \
  --backend nats --nats-url nats://127.0.0.1:4222 --workers 2 --worker-index 0
# terminal 2
cargo run -p headrace -- run examples/latency.yaml \
  --backend nats --nats-url nats://127.0.0.1:4222 --workers 2 --worker-index 1
```

Payloads are MessagePack. Decode one to JSON with `msgpack2json` from
[`msgpack-tools`](https://github.com/ludocode/msgpack-tools) (reads stdin); `--raw` prints just
the payload and `--count 1` takes a single message:

```shell
brew install msgpack-tools
nats sub --raw --count 1 'hr.>' | msgpack2json
```

The end-to-end backend test spins up NATS itself via testcontainers; it is `#[ignore]`d, so it
needs Docker and `--ignored`:

```shell
cargo test -p headrace-core --features nats --test nats_e2e -- --ignored --nocapture
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
