# 0013. Web presence: hand-rolled landing, Vocs docs, Cloudflare Pages

- Status: Accepted
- Date: 2026-08-07

## Context

The prose docs were good but scattered with no navigable home: `README.md`, `DESIGN.md`, the
per-transform guides (`docs/windowing.md`, `docs/map.md`), 12 ADRs, and per-crate READMEs. A
docs site is a v0.4 roadmap item (`DESIGN.md#roadmap`). Two distinct needs surfaced:

- **Docs** - navigable, searchable prose with good code rendering. Should reuse the existing
  markdown, keep `DESIGN.md` canonical (AGENTS.md), and leave rustdoc -> docs.rs as the API
  reference.
- **A landing page** - a marketing home that leads with the value (pre-aggregate telemetry,
  ship less downstream). This is a different craft from docs, and a docs engine makes a poor
  marketing-page builder.

Constraint: deploy as a static site to a low-cost, Git-integrated host on a custom domain.

## Decision

**Split the two concerns, serve them from one domain.**

- **Docs: Vocs** (Vite/React/MDX, waku-based). Content lives in `docs/src/pages`;
  `renderStrategy: 'full-static'` prerenders every page to HTML; `basePath: '/docs'` serves it
  under `/docs`. Brand-themed via CSS-variable overrides (the pipeline-diagram palette). The
  transform guides render as pages; `DESIGN.md` stays the canonical architecture doc and is
  linked, not forked. Mermaid, when needed, is a `rehype-mermaid` plugin.
- **Landing: hand-rolled** static HTML/CSS (+ a few lines of JS) in `site/`, owning the root.
  It is one page, so no framework earns its keep yet; if it grows to several pages we move it to
  **Hugo** (familiar, huge community; theme ecosystem is moot since the design is bespoke). The
  page is brand-themed and light/dark.
- **Assembly:** `scripts/build-web.sh` builds the docs and assembles `dist/` - landing at `/`,
  `docs/dist/public` at `/docs`.
- **Hosting: Cloudflare Pages** on **`headrace.rs`**, via Cloudflare's Git integration: it builds
  `scripts/build-web.sh` and publishes `dist/` on each push - `main` to production, PRs get
  `*.pages.dev` previews. (A GitHub Actions + `wrangler` deploy was the first plan, but it meant
  managing Cloudflare secrets in the repo and a second, redundant build; the Git integration needs
  neither.) Netlify was the fallback; Cloudflare was chosen.
- **ADRs stay repo-internal** in `docs/adr/` - they are decision records, not site content, so
  they are not rendered on the site (this reverses the earlier draft that moved them under the
  site).

```text
site/                     # hand-rolled landing (root) - no build
  index.html  styles.css  *.svg
docs/                     # Vocs docs (served at /docs)
  vocs.config.ts          # renderStrategy: full-static, basePath: /docs, brand theme
  src/pages/
    index.mdx             # docs overview
    getting-started.mdx
    transforms/{filter,map,window}.md
scripts/build-web.sh      # docs build + assemble -> dist/
dist/                     # combined output (git-ignored): / = landing, /docs = docs
```

## Consequences

- The docs bring a Node/Vite/React toolchain (`docs/package.json` + lockfile) and a CI job; the
  landing stays zero-build (a folder of files). Editing docs prose is still plain markdown.
- One domain, one Cloudflare Pages project, one build - the combined `dist/` is what ships.
  Host and domain are swappable: any static host consumes the same `dist/`.
- `DESIGN.md` stays canonical, so docs Concept pages link into it rather than duplicating it;
  docs.rs remains the API reference and is never duplicated.
- The transform guides now exist both as the originals (`docs/windowing.md`, `docs/map.md`) and as
  migrated pages under `docs/src/pages/transforms/`. That duplication is temporary, to be
  reconciled (delete the originals, fix the README links) in a follow-up.
- Hand-rolled means no partials/templating on the landing; the day it needs a second page, the
  HTML/CSS lifts into Hugo with little change (a working Hugo build of this exact page was
  prototyped).
