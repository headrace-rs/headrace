#!/usr/bin/env bash
# Build the combined public site: the hand-rolled landing at /, the Vocs docs at /docs.
# Output goes to dist/ (git-ignored), which is what Cloudflare Pages publishes.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

# 1. Build the Vocs docs. basePath=/docs, so its assets/links are prefixed accordingly.
( cd docs && npm ci && npm run build )

# 2. Assemble: landing files at the root, docs under /docs.
rm -rf dist
mkdir -p dist/docs
cp -R site/. dist/
cp -R docs/dist/public/. dist/docs/

echo "built -> dist/  (landing at /, docs at /docs)"
