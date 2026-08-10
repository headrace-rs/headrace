#!/usr/bin/env bash
# Watch the edge aggregation happen in real time: raw samples stream in, and every 3s a
# window closes and emits one average-latency datapoint per service. Runs ~12s, then stops.
set -euo pipefail
cd "$(dirname "$0")/.."

if command -v headrace >/dev/null 2>&1; then
	bin=(headrace)
elif [ -x target/release/headrace ]; then
	bin=(target/release/headrace)
else
	bin=(cargo run -q -p headrace --)
fi

echo "==> streaming raw samples through headrace (stdin -> 3s window -> stdout)" >&2
python3 demo/feed_live.py | "${bin[@]}" --log error run demo/live.yaml
