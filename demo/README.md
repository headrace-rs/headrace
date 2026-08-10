# Demo: cut telemetry egress at the edge

Headrace's pitch in one command. It pre-aggregates a high-cardinality metric stream so that
only a small fraction of it - the part you actually chart and alert on - leaves the pipeline.
Everything else never has to be shipped or stored downstream, which is where the bill is.

```sh
./demo/run.sh
```

It feeds a synthetic raw stream (`http.server.duration`, tagged by service x route x pod x
status) into a Headrace pipeline that rolls it up to average latency per service and route
over 10s windows, then shows one series end to end and how much smaller the result is:

```
  svc-2 /api/r9:
    raw    value=23.43   {service.name=svc-2, http.route=/api/r9, pod=pod-0, status=404}
           ... 646 raw samples across pods and statuses
    roll   value=30.1    window=[0s,10s)  {service.name=svc-2, http.route=/api/r9}
    ...
    roll   value=30.4    window=[50s,60s)  {service.name=svc-2, http.route=/api/r9}
           -> 6 windowed averages (pod and status dropped)

  datapoints     60,000  ->     576   (104x fewer)
  series          1,920  ->      96   (20x fewer)
```

No external services: the pipeline is just `stdin -> window -> stdout`. In production the
edges are OTLP instead of stdin/stdout, but the reduction is identical - that is the point.

## Watch it live

`run.sh` replays a file at full speed, so the whole 60k finishes in well under a second. To
watch windows fire in real time instead, stream a paced feed:

```sh
./demo/live.sh
```

Raw samples flow in continuously and, every 3s, a window closes and prints one average per
service - the same aggregation, paced so you can see it happen. It runs ~12s and stops.

## Knobs

Tune the input with env vars (defaults in parens): `SERVICES` (8), `ROUTES` (12), `PODS` (5),
`STATUSES` (4), `TOTAL` (60000) datapoints, `DURATION_S` (60) of event time. For example, a
denser stream over a longer span:

```sh
TOTAL=500000 DURATION_S=300 ./demo/run.sh
```

The pipeline is [`pipeline.yaml`](./pipeline.yaml) and the stream generator is
[`feed.py`](./feed.py). The demo uses `cargo run` from a checkout, or an installed `headrace`
binary if one is on your `PATH`.
