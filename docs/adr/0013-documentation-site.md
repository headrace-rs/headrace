# 0013. Documentation site: Vocs

- Status: Proposed
- Date: 2026-08-07

## Context

The prose docs are good but scattered with no navigable home: `README.md`, `DESIGN.md`, the
per-transform guides (`docs/windowing.md`, `docs/map.md`), 12 ADRs, and per-crate READMEs. A docs
site is a v0.4 roadmap item (`DESIGN.md#roadmap`). Forces on the choice:

- **Reuse, not rewrite.** The existing markdown should render with minimal churn.
- **One source of truth.** `DESIGN.md` is the canonical architecture doc and must stay in sync
  (AGENTS.md); the site must link into it, not fork it.
- **Mermaid.** `DESIGN.md` carries 7 mermaid diagrams, and AGENTS.md encourages more, so the
  renderer must handle mermaid.
- **Brand.** It should carry the teal + H-mark identity (`brand/`) and the pipeline diagram
  (`docs/assets/pipeline.svg`).
- **API docs are already solved.** rustdoc -> docs.rs owns the API reference; the site owns
  concepts, guides, and decisions only.
- **Deploy.** A static site on a low-cost, Git-integrated host, ideally on a custom domain.

Options weighed: **mdBook** (Rust-native, zero Node, first-class `mdbook-mermaid`, but plain and no
MDX/interactivity), **Vocs** (Vite/React/MDX, strong theming and built-in search, MDX leaves room
for interactive IR/schema docs later, but adds a Node toolchain and needs a mermaid plugin), and
**Starlight** (Astro; a middle ground). We accept a Node toolchain in exchange for theming that
matches the brand and headroom for interactive docs.

## Decision

We will build the docs site with **Vocs**.

- **Location.** `docs/` becomes the site root, content under `docs/pages/`. `windowing.md`,
  `map.md`, and `adr/` move under `pages/`; the handful of relative links (README, cross-links,
  `adr/README.md` index) get a one-time fixup.
- **Reuse.** The transform guides and the ADRs render as-is with added frontmatter. The landing
  page reuses the README hero and the pipeline SVG, which stands in for a landing diagram.
- **Canonical sources stay put.** `DESIGN.md` remains authoritative and renders as one page;
  Concept pages summarize and link into it rather than copying, so there is a single source of
  truth. The site links out to docs.rs for the API.
- **Mermaid** renders via a `rehype-mermaid` plugin (or a small MDX `<Mermaid>` component) so the
  `DESIGN.md` diagrams work.
- **Theme.** Accent teal (`#0E9AA0` light, `#2DD4BF` dark), H-mark logo from `brand/assets`,
  light and dark.
- **Hosting.** Deploy the static build behind a Git-integrated host with per-PR preview
  deployments - **Cloudflare Pages** preferred, **Netlify** the fallback if cost or features
  dictate - on the custom domain **`headrace.rs`** (pending registration; `.rs` also nods to the
  `headrace-rs` org and to Rust). The host builds on push, so no deploy workflow lives in the repo;
  an optional CI job runs the Vocs build to catch breakage (Sentence-case names, AGENTS.md).

```text
docs/
  vocs.config.ts          # accent teal, logo, sidebar
  package.json
  assets/pipeline.svg     # existing; landing hero
  pages/
    index.mdx             # hero + quickstart + feature grid (from README)
    getting-started.md
    concepts/*.md         # links into DESIGN.md
    transforms/{map,windowing}.md   # moved, reused as-is
    adr/*.md              # moved, reused as-is
```

## Consequences

- Introduces a Node/Vite/React toolchain into an otherwise Rust repo: a `package.json` and a
  lockfile. Editing prose still means editing plain markdown; Node is needed only to preview or
  build the site.
- Mermaid is a plugin rather than first-class, the main cost of choosing Vocs over mdBook.
- Moving `docs/windowing.md`, `docs/map.md`, and `docs/adr/` under `docs/pages/` changes their
  paths, so README links and the ADR index need a one-time update.
- Keeping `DESIGN.md` canonical avoids drift but means Concept pages link rather than fully inline
  the architecture.
- MDX leaves room for interactive docs later (an IR JSON-Schema explorer, runnable pipeline
  snippets) without another migration.
- docs.rs stays the source of truth for API docs; the site never duplicates it.
- Host and domain are the open items: Cloudflare Pages vs Netlify is a cost call, and `headrace.rs`
  depends on registration. Both hosts consume the same static build, so the choice is low-risk and
  reversible, and a different domain changes only DNS.
