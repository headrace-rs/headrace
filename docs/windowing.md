# Windowing

The `window` transform groups records by `group_by` and reduces each group over a time
window. Windows are placed in **event time** - each record's own `ts_nanos` (OTel
`TimeUnixNano`), not the wall clock - so rollups stay correct under lag, batching, and
replay.

## Watermark

A window `[start, end)` **fires** (emits one record per group, then drops its state) when
the watermark reaches its `end`:

```
watermark = max_event_time_seen - allowed_lateness
```

The watermark is the point in event time up to which input is treated as complete. It
trails the newest event seen by `allowed_lateness`, so a larger grace fires later.

## Tumbling windows

Fixed-size, non-overlapping, aligned to the epoch; every record lands in exactly one
window. With `size: 5s`:

```
event time (seconds) ->
   0         5         10        15
   |---------|---------|---------|
   |  [0,5)  | [5,10)  | [10,15) |
      a b c     d   e       f
   emits:  avg(a,b,c)  avg(d,e)  avg(f)
```

A record at `t=5` opens `[5,10)` and advances `max_event_time` to 5; the watermark then
reaches the end of `[0,5)`, which fires.

## Sliding windows

Overlapping windows of `size`, advanced by a smaller `slide`, so one record can land in
several. With `size: 10s`, `slide: 5s`:

```
event time (seconds) ->
   0         5         10        15        20
   |---------|---------|---------|---------|
   [------ [0,10) -----)
             [------ [5,15) -----)
                       [----- [10,20) -----)
```

A record at `t=7` contributes to both `[0,10)` and `[5,15)`.

## allowed_lateness

Real sources deliver out of order. `allowed_lateness` holds a window open past its `end`
(in event time) so late-but-not-too-late records still count. With `size: 5s` and
`allowed_lateness: 2s`, window `[0,5)` fires only once `max_event_time >= 7`:

```
arrival (in event time):  t=1   t=3   t=6   t=4   t=8
  t=1, t=3  -> fold into [0,5)
  t=6       -> opens [5,10); watermark = 6-2 = 4, so [0,5) stays open
  t=4       -> late, but [0,5) is still open within the grace -> folded in
  t=8       -> watermark = 8-2 = 6 >= 5, so [0,5) fires: avg(1,3,4)
```

A record whose window has already fired is **dropped** and counted on
`headrace.records.late`. A nonzero late rate means `allowed_lateness` is too small for the
source's out-of-orderness.

## idle_timeout

The watermark only advances when records arrive, so a stream that goes quiet leaves its
last windows open. Set `idle_timeout` to force every open window to flush after that much
wall-clock silence. Off by default (windowing stays purely event-time); a clean shutdown
still flushes open windows regardless.

## Configuration

```yaml
transforms:
  - type: window
    id: rollup
    input: in
    name: "req.latency.avg" # rename the emitted metric (optional)
    size: 5s               # tumbling window length (required)
    allowed_lateness: 2s   # event-time grace before firing (optional, default 0s)
    idle_timeout: 30s      # flush open windows after this quiet (optional, default off)
    group_by: [service.name, http.route]
    aggregate:
      op: avg              # count | sum | min | max | avg
      field: value         # numeric attribute to reduce (default: the record's value)
      on_missing: skip     # skip | error, when `field` is absent
      on_invalid: error    # skip | error, when `field` is present but non-numeric
```
