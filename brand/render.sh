#!/usr/bin/env bash
# Re-render the brand PNGs from the source SVGs.
# Requires the resvg CLI: `cargo install resvg`
set -euo pipefail
cd "$(dirname "$0")/assets"

resvg --width 512 logo.svg        logo.png
resvg --width 512 logo-dark.svg   logo-dark.png
resvg --width 512 avatar.svg      avatar.png
resvg --width 32  favicon.svg     favicon-32.png
resvg --width 16  favicon.svg     favicon-16.png

echo "rendered: logo.png logo-dark.png avatar.png favicon-32.png favicon-16.png"
