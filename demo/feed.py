#!/usr/bin/env python3
"""Generate a high-cardinality raw metric stream for the Headrace edge-aggregation demo.

Emits one JSON `Record` per line on stdout (the shape `headrace run` reads from a `stdin`
source): an `http.server.duration` sample tagged by service x route x pod x status, with
event-time timestamps spread across the window span. Deterministic (seeded) so the numbers
are reproducible. Tune with the env vars below.
"""
import json
import os
import random
import sys


def count(name, default):
    return int(os.environ.get(name, default))


services = [f"svc-{i}" for i in range(count("SERVICES", 8))]
routes = [f"/api/r{i}" for i in range(count("ROUTES", 12))]
pods = [f"pod-{i}" for i in range(count("PODS", 5))]
statuses = ["200", "204", "404", "500"][: count("STATUSES", 4)]
total = count("TOTAL", 60000)
span_ns = int(float(os.environ.get("DURATION_S", 60)) * 1_000_000_000)

random.seed(1)
step = max(span_ns // max(total, 1), 1)
series = set()
out = sys.stdout
for i in range(total):
    svc, route = random.choice(services), random.choice(routes)
    pod, status = random.choice(pods), random.choice(statuses)
    series.add((svc, route, pod, status))
    rec = {
        "ts_nanos": i * step,
        "name": "http.server.duration",
        "value": round(random.lognormvariate(3.2, 0.6), 2),  # latency, ms
        "attrs": {
            "service.name": svc,
            "http.route": route,
            "pod": pod,
            "status": status,
        },
    }
    out.write(json.dumps(rec))
    out.write("\n")

print(
    f"raw: {total} datapoints across {len(series)} series "
    f"({len(services)} svc x {len(routes)} route x {len(pods)} pod x {len(statuses)} status)",
    file=sys.stderr,
)
