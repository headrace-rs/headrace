#!/usr/bin/env python3
"""Paced feeder for the realtime demo. Emits raw `http.server.duration` samples in real time
(event time follows the wall clock, so windows fire live) and prints a running raw count to
stderr. Stops after DURATION_S. Tune with RATE, SERVICES, ROUTES, PODS, DURATION_S.
"""
import json
import os
import random
import sys
import time

services = [f"svc-{i}" for i in range(int(os.environ.get("SERVICES", 8)))]
routes = [f"/api/r{i}" for i in range(int(os.environ.get("ROUTES", 6)))]
pods = [f"pod-{i}" for i in range(int(os.environ.get("PODS", 5)))]
statuses = ["200", "204", "404", "500"]
duration = float(os.environ.get("DURATION_S", 12))
rate = int(os.environ.get("RATE", 1200))  # raw samples per second

random.seed(1)
start = time.monotonic()
n, last_report = 0, 0.0
while True:
    now = time.monotonic() - start
    if now >= duration:
        break
    rec = {
        "ts_nanos": int(now * 1_000_000_000),
        "name": "http.server.duration",
        "value": round(random.lognormvariate(3.2, 0.6), 2),
        "attrs": {
            "service.name": random.choice(services),
            "http.route": random.choice(routes),
            "pod": random.choice(pods),
            "status": random.choice(statuses),
        },
    }
    sys.stdout.write(json.dumps(rec) + "\n")
    sys.stdout.flush()
    n += 1
    if now - last_report >= 1.0:
        last_report = now
        print(f"  ... {n:,} raw samples in", file=sys.stderr, flush=True)
    time.sleep(1.0 / rate)

print(f"  {n:,} raw samples in total", file=sys.stderr, flush=True)
