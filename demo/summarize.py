#!/usr/bin/env python3
"""Summarize the Headrace edge-aggregation demo.

Shows one service/route's raw samples collapsing into a handful of windowed averages, then
the overall datapoint and series reduction. Usage: summarize.py <raw.jsonl> <agg.jsonl>.
"""
import json
import sys


def records(path):
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def series(recs, keys):
    return {tuple(r.get("attrs", {}).get(k) for k in keys) for r in recs}


def attrs(a, keys):
    return ", ".join(f"{k}={a.get(k)}" for k in keys)


raw = list(records(sys.argv[1]))
agg = list(records(sys.argv[2]))

raw_series = series(raw, ["service.name", "http.route", "pod", "status"])
agg_series = series(agg, ["service.name", "http.route"])

# One (service, route) shown end to end, so the rollup is visible - not just counts.
def route_of(r):
    a = r.get("attrs", {})
    return (a.get("service.name"), a.get("http.route"))


pick = route_of(raw[0])
raw_pick = [r for r in raw if route_of(r) == pick]
agg_pick = [r for r in agg if route_of(r) == pick]

print()
print(f"  {pick[0]} {pick[1]}:")
r0 = raw_pick[0]
print(
    f"    raw    value={r0['value']:<6}  "
    f"{{{attrs(r0['attrs'], ['service.name', 'http.route', 'pod', 'status'])}}}"
)
print(f"           ... {len(raw_pick):,} raw samples across pods and statuses")
for r in agg_pick:
    start = (r.get("start_ts_nanos") or 0) // 1_000_000_000
    end = r["ts_nanos"] // 1_000_000_000
    print(
        f"    roll   value={r['value']:<6.1f}  window=[{start}s,{end}s)  "
        f"{{{attrs(r['attrs'], ['service.name', 'http.route'])}}}"
    )
print(f"           -> {len(agg_pick)} windowed averages (pod and status dropped)")

print()
print(f"  datapoints   {len(raw):>8,}  ->  {len(agg):>6,}   ({len(raw) / max(len(agg), 1):.0f}x fewer)")
print(f"  series       {len(raw_series):>8,}  ->  {len(agg_series):>6,}   ({len(raw_series) / max(len(agg_series), 1):.0f}x fewer)")
print()
print("  That reduction is what you no longer ship or pay to store downstream.")
