#!/usr/bin/env bash
# One-command demo: feed a high-cardinality raw metric stream through Headrace's edge
# aggregation and show how much less leaves the pipeline. No external services.
set -euo pipefail
cd "$(dirname "$0")/.."

raw="$(mktemp)"
agg="$(mktemp)"
trap 'rm -f "$raw" "$agg"' EXIT

echo "==> generating a high-cardinality raw metric stream" >&2
python3 demo/feed.py >"$raw"

echo "==> pre-aggregating through headrace (stdin -> 10s window -> stdout)" >&2
# --log error keeps Headrace's own startup logs out of the demo output.
if command -v headrace >/dev/null 2>&1; then
	headrace --log error run demo/pipeline.yaml <"$raw" >"$agg"
else
	cargo run -q -p headrace -- --log error run demo/pipeline.yaml <"$raw" >"$agg"
fi

python3 demo/summarize.py "$raw" "$agg"
