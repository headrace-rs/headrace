//! `window`: group records by `group_by` and reduce each group over a tumbling time
//! window. Windows are placed in *event time* and fired on a watermark. Stateful.
//!
//! [`Window`] is the pure, synchronous core (fold + fire); [`run`] is the async driver
//! that owns only I/O.

use crate::backend::{Consumer, Producer};
use crate::inspect::{GroupSnapshot, Inspect, NodeSnapshot, labels_of, publish, recv_query};
use crate::metrics::NodeMetrics;
use crate::record::{AttrValue, Attrs, Fault, Record};
use anyhow::{Result, bail};
use headrace_ir::{Aggregate, AggregateOp, FaultAction};
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

/// A key part per `group_by` dimension, in the transform's declared order.
///
/// Canonical and hashable: types stay distinct (`Int(1)` != `Str("1")`) and a
/// missing attribute (`Absent`) differs from an empty string, so distinct groups
/// never collide. This is also the identity that partitions state across workers
/// in the scaled deployment (DESIGN.md: group_key -> partition -> worker-local state).
#[derive(Clone, PartialEq, Eq, Hash)]
enum KeyPart {
    Bool(bool),
    Int(i64),
    /// f64 bit pattern, so the key stays `Eq + Hash`.
    Double(u64),
    Str(String),
    Absent,
}

impl KeyPart {
    fn of(v: Option<&AttrValue>) -> Self {
        match v {
            Some(AttrValue::Bool(b)) => KeyPart::Bool(*b),
            Some(AttrValue::Int(i)) => KeyPart::Int(*i),
            Some(AttrValue::Double(d)) => KeyPart::Double(d.to_bits()),
            Some(AttrValue::Str(s)) => KeyPart::Str(s.clone()),
            None => KeyPart::Absent,
        }
    }
}

type GroupKey = Vec<KeyPart>;

struct Agg {
    name: String,
    attrs: Attrs,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
}

impl Agg {
    fn new(name: String, attrs: Attrs) -> Self {
        Self {
            name,
            attrs,
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn add(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
    }

    /// `None` for an empty group on ops that need a sample (min/max/avg); an empty
    /// group would otherwise emit `+/-INFINITY`. Count of an empty group is `0`.
    fn value(&self, op: AggregateOp) -> Option<f64> {
        if self.count == 0 {
            return match op {
                AggregateOp::Count => Some(0.0),
                _ => None,
            };
        }
        Some(match op {
            AggregateOp::Count => self.count as f64,
            AggregateOp::Sum => self.sum,
            AggregateOp::Min => self.min,
            AggregateOp::Max => self.max,
            AggregateOp::Avg => self.sum / self.count as f64,
        })
    }
}

/// The stateful core of the window transform: pure, synchronous, testable in isolation.
///
/// Records are placed into tumbling windows by their own `ts_nanos` (event time),
/// aligned to the epoch on `size`. Many windows can be open at once. A window `[start,
/// end)` fires when the watermark - `max_event_time - allowed_lateness` - reaches its
/// `end`; a record whose window has already fired is late and dropped. The async driver
/// ([`run`]) owns only I/O.
pub struct Window {
    size_nanos: u64,
    /// Step between window starts. Equal to `size_nanos` for tumbling; smaller for
    /// sliding, where a record falls in `ceil(size / slide)` overlapping windows.
    slide_nanos: u64,
    lateness_nanos: u64,
    group_by: Vec<String>,
    aggregate: Aggregate,
    /// Renames the emitted metric; `None` keeps each group's source name.
    name: Option<String>,
    /// Open windows keyed by start; each holds its per-group accumulators. Ordered so
    /// the earliest-ready windows fire first.
    windows: BTreeMap<u64, HashMap<GroupKey, Agg>>,
    /// Cap on distinct groups per open window; `None` is unbounded.
    max_groups: Option<usize>,
    max_event: u64,
    skipped: u64,
    late: u64,
    /// Records refused because their window was already at `max_groups`.
    capped: u64,
}

/// Parsed, typed settings for a [`Window`]. Build a `Window` with `Window::from` /
/// `.into()` rather than positional arguments, so the three durations can't be silently
/// transposed at a call site.
pub struct WindowConfig {
    /// Window length.
    pub size: Duration,
    /// Step between window starts: equal to `size` for tumbling, smaller for sliding.
    pub slide: Duration,
    /// Grace period, in event time, to hold a window open past its end for late records.
    pub lateness: Duration,
    /// Attribute keys whose tuple identifies a group; empty aggregates the whole stream.
    pub group_by: Vec<String>,
    pub aggregate: Aggregate,
    /// Renames the emitted metric; `None` keeps each group's source name.
    pub name: Option<String>,
    /// Cap on distinct groups per open window; `None` is unbounded.
    pub max_groups: Option<usize>,
}

impl WindowConfig {
    /// A tumbling window (`slide == size`) with no allowed lateness and no grouping - the
    /// common single-series case. Override fields with struct-update syntax:
    /// `WindowConfig { lateness, ..WindowConfig::tumbling(size, agg) }`.
    pub fn tumbling(size: Duration, aggregate: Aggregate) -> Self {
        Self {
            size,
            slide: size,
            lateness: Duration::ZERO,
            group_by: Vec::new(),
            aggregate,
            name: None,
            max_groups: None,
        }
    }
}

impl From<WindowConfig> for Window {
    fn from(cfg: WindowConfig) -> Self {
        let nanos = |d: Duration| d.as_nanos() as u64;
        Self {
            size_nanos: nanos(cfg.size),
            slide_nanos: nanos(cfg.slide),
            lateness_nanos: nanos(cfg.lateness),
            group_by: cfg.group_by,
            aggregate: cfg.aggregate,
            name: cfg.name,
            windows: BTreeMap::new(),
            max_groups: cfg.max_groups,
            max_event: 0,
            skipped: 0,
            late: 0,
            capped: 0,
        }
    }
}

impl Window {
    /// Event time up to which input is treated as complete: the newest event seen, less
    /// the allowed lateness.
    fn watermark(&self) -> u64 {
        self.max_event.saturating_sub(self.lateness_nanos)
    }

    /// Starts of every window containing event time `t`. Tumbling (`slide == size`)
    /// yields one; sliding yields the overlapping windows, newest first.
    fn window_starts(&self, t: u64) -> Vec<u64> {
        let (size, slide) = (self.size_nanos, self.slide_nanos);
        let mut start = (t / slide) * slide; // newest slide-aligned start <= t
        let mut starts = Vec::new();
        loop {
            starts.push(start);
            if start < slide {
                break; // reached the epoch-aligned first window
            }
            start -= slide;
            if start + size <= t {
                break; // this earlier window no longer contains t
            }
        }
        starts
    }

    /// Fold one record into every event-time window it belongs to (one for tumbling,
    /// several for sliding). Windows that have already fired are skipped; a record whose
    /// windows have all fired is counted late. `Err` only under `FaultAction::Error`.
    pub fn on_record(&mut self, rec: &Record) -> Result<()> {
        let v = match rec.numeric(self.aggregate.field.as_deref()) {
            Ok(v) => v,
            Err(fault) => {
                let policy = match fault {
                    Fault::Missing => self.aggregate.on_missing,
                    Fault::Invalid => self.aggregate.on_invalid,
                };
                match policy {
                    FaultAction::Skip => {
                        self.skipped += 1;
                        // A skipped record still advances the stream's event time.
                        self.max_event = self.max_event.max(rec.ts_nanos);
                        return Ok(());
                    }
                    FaultAction::Error => {
                        let field = self.aggregate.field.as_deref().unwrap_or("value");
                        match fault {
                            Fault::Missing => bail!("record missing numeric field `{field}`"),
                            Fault::Invalid => bail!("record has non-numeric field `{field}`"),
                        }
                    }
                }
            }
        };
        let watermark = self.watermark();
        let cap = self.max_groups;
        let (key, attrs) = group_key(rec, &self.group_by);
        let mut folded = false;
        let mut capped = false;
        for start in self.window_starts(rec.ts_nanos) {
            if start + self.size_nanos <= watermark {
                continue; // this window has already fired
            }
            let groups = self.windows.entry(start).or_default();
            if let Some(agg) = groups.get_mut(&key) {
                agg.add(v); // an existing group always updates, regardless of the cap
                folded = true;
            } else if cap.is_some_and(|c| groups.len() >= c) {
                capped = true; // window at its group cap; refuse this new group
            } else {
                groups
                    .entry(key.clone())
                    .or_insert_with(|| Agg::new(rec.name.clone(), attrs.clone()))
                    .add(v);
                folded = true;
            }
        }
        if folded {
            self.max_event = self.max_event.max(rec.ts_nanos);
        } else if capped {
            // A shed record still arrived, so it advances event time, exactly as a skipped one
            // does above - a saturated window must not stall the watermark.
            self.max_event = self.max_event.max(rec.ts_nanos);
            self.capped += 1;
        } else {
            self.late += 1;
        }
        Ok(())
    }

    /// Records skipped (missing/non-numeric field) since the last call.
    pub fn drain_skipped(&mut self) -> u64 {
        std::mem::take(&mut self.skipped)
    }

    /// Records dropped as late (their window had already fired) since the last call.
    pub fn drain_late(&mut self) -> u64 {
        std::mem::take(&mut self.late)
    }

    /// Records refused since the last call because their window was at `max_groups`.
    pub fn drain_capped(&mut self) -> u64 {
        std::mem::take(&mut self.capped)
    }

    /// Emit and remove every window the watermark has passed.
    pub fn drain_ready(&mut self) -> Vec<Record> {
        let watermark = self.watermark();
        self.drain_windows(|end| end <= watermark)
    }

    /// Emit and remove all open windows regardless of the watermark - the final flush
    /// when the input closes, or an idle-timeout collapse.
    pub fn drain_all(&mut self) -> Vec<Record> {
        self.drain_windows(|_| true)
    }

    /// Remove the windows whose `end` satisfies `ready`, emitting one record per
    /// non-empty group for each.
    fn drain_windows(&mut self, ready: impl Fn(u64) -> bool) -> Vec<Record> {
        let op = self.aggregate.op;
        let size = self.size_nanos;
        let name = self.name.clone();
        let starts: Vec<u64> = self
            .windows
            .keys()
            .copied()
            .filter(|&start| ready(start + size))
            .collect();
        let mut out = Vec::new();
        for start in starts {
            let end = start + size;
            let groups = self.windows.remove(&start).expect("start came from keys()");
            for (_, a) in groups {
                let Some(value) = a.value(op) else { continue };
                out.push(Record {
                    ts_nanos: end,
                    start_ts_nanos: Some(start),
                    resource: Attrs::new(),
                    scope: None,
                    name: name.clone().unwrap_or(a.name),
                    value,
                    attrs: a.attrs,
                });
            }
        }
        out
    }

    /// A read-only view of every open (not-yet-fired) window and group, for state
    /// inspection (ADR-0014). Reports each group's running aggregate and the number of
    /// records folded so far; it fires and mutates nothing.
    pub fn snapshot(&self) -> NodeSnapshot {
        let op = self.aggregate.op;
        let mut groups = Vec::new();
        for (&start, by_key) in &self.windows {
            let end = start + self.size_nanos;
            for a in by_key.values() {
                groups.push(GroupSnapshot {
                    labels: labels_of(&a.attrs),
                    start_nanos: start,
                    end_nanos: end,
                    value: a.value(op),
                    inputs: std::collections::BTreeMap::new(),
                    samples: a.count,
                });
            }
        }
        NodeSnapshot {
            kind: "window",
            groups,
        }
    }
}

fn group_key(rec: &Record, keys: &[String]) -> (GroupKey, Attrs) {
    let mut key = Vec::with_capacity(keys.len());
    let mut attrs = Attrs::new();
    for k in keys {
        let v = rec.lookup(k);
        key.push(KeyPart::of(v));
        if let Some(v) = v {
            attrs.insert(k.clone(), v.clone());
        }
    }
    (key, attrs)
}

/// The window transform's settings, parsed from the IR node.
pub(super) struct Spec {
    pub size: String,
    pub slide: Option<String>,
    pub allowed_lateness: Option<String>,
    pub idle_timeout: Option<String>,
    pub group_by: Vec<String>,
    pub aggregate: Aggregate,
    pub name: Option<String>,
    pub max_groups: Option<usize>,
}

/// Drive the window in event time: fold records, firing each window when the watermark
/// (`max_event_time - allowed_lateness`) reaches its end. With `idle_timeout` set, a spell
/// of that long with no records collapses all open windows, so a stream that goes quiet
/// still emits. Windows still open when the upstream closes are flushed best-effort.
pub(super) async fn run(
    spec: Spec,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
    mut inspect: Option<Inspect>,
) -> Result<()> {
    let Spec {
        size,
        slide,
        allowed_lateness,
        idle_timeout,
        group_by,
        aggregate,
        name,
        max_groups,
    } = spec;
    let size = humantime::parse_duration(&size)?;
    let slide = match &slide {
        Some(s) => humantime::parse_duration(s)?,
        None => size, // tumbling
    };
    let lateness = match &allowed_lateness {
        Some(l) => humantime::parse_duration(l)?,
        None => Duration::ZERO,
    };
    let idle = match &idle_timeout {
        Some(t) => Some(humantime::parse_duration(t)?),
        None => None,
    };
    let mut win = Window::from(WindowConfig {
        size,
        slide,
        lateness,
        group_by,
        aggregate,
        name,
        max_groups,
    });

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(rec) => {
                    win.on_record(&rec)?;
                    meter_drops(&mut win, &nm);
                    if !emit(win.drain_ready(), tx.as_ref(), &nm).await {
                        return Ok(());
                    }
                    publish(&inspect, || win.snapshot());
                }
                None => break,
            },
            // Fires only when `idle` is set; otherwise this branch never completes.
            _ = maybe_sleep(idle) => {
                if !emit(win.drain_all(), tx.as_ref(), &nm).await {
                    return Ok(());
                }
                publish(&inspect, || win.snapshot());
            }
            // Fires only when inspection is on; answers a snapshot query from the node's
            // own loop, so the reply is consistent with the folds above.
            query = recv_query(&mut inspect) => match query {
                Some(reply) => { let _ = reply.send(win.snapshot()); }
                None => inspect = None, // every Handle dropped; stop polling
            },
        }
    }
    // Upstream closed cleanly: flush whatever is still open.
    meter_drops(&mut win, &nm);
    emit(win.drain_all(), tx.as_ref(), &nm).await;
    Ok(())
}

/// Sleep for `d`, or - when no idle timeout is configured - never complete, so the
/// select's idle branch stays dormant and windowing is purely event-time.
async fn maybe_sleep(d: Option<Duration>) {
    match d {
        Some(d) => tokio::time::sleep(d).await,
        None => std::future::pending().await,
    }
}

/// Meter and log records dropped since the last call: skipped for a missing field, or
/// late because their window had already fired.
fn meter_drops(win: &mut Window, nm: &NodeMetrics) {
    let skipped = win.drain_skipped();
    if skipped > 0 {
        tracing::warn!(
            skipped,
            "window: dropped records with missing/non-numeric field"
        );
        nm.dropped(skipped);
    }
    let late = win.drain_late();
    if late > 0 {
        tracing::warn!(late, "window: dropped records past allowed_lateness");
        nm.late(late);
    }
    let capped = win.drain_capped();
    if capped > 0 {
        tracing::warn!(capped, "window: dropped records over max_groups");
        nm.capped(capped);
    }
}

/// Forward fired records downstream. Returns `false` if the downstream is gone.
async fn emit(recs: Vec<Record>, tx: &dyn Producer, nm: &NodeMetrics) -> bool {
    if recs.is_empty() {
        return true;
    }
    nm.window_flushed(recs.len() as u64);
    for rec in recs {
        nm.out();
        if tx.send(rec).await.is_err() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIZE: u64 = 1000;

    // --- aggregate math ---

    #[test]
    fn aggregate_ops() {
        let vs = [2.0, 4.0, 9.0];
        assert_eq!(one_group(AggregateOp::Count, &vs), 3.0);
        assert_eq!(one_group(AggregateOp::Sum, &vs), 15.0);
        assert_eq!(one_group(AggregateOp::Min, &vs), 2.0);
        assert_eq!(one_group(AggregateOp::Max, &vs), 9.0);
        assert_eq!(one_group(AggregateOp::Avg, &vs), 5.0);
    }

    #[test]
    fn drain_emits_window_bounds_and_resets() {
        let mut w = win(AggregateOp::Sum, None, FaultAction::Skip, &[]);
        w.on_record(&rec("m", 1.0, &[])).unwrap(); // ts 1 -> window [0, SIZE)
        let out = w.drain_all();
        assert_eq!(out[0].start_ts_nanos, Some(0));
        assert_eq!(out[0].ts_nanos, SIZE);
        // Drained: a second flush with no input emits nothing.
        assert!(w.drain_all().is_empty());
    }

    // --- grouping ---

    #[test]
    fn groups_split_by_key_and_carry_attrs() {
        let mut w = win(
            AggregateOp::Count,
            None,
            FaultAction::Skip,
            &["service.name"],
        );
        for svc in ["a", "a", "b"] {
            w.on_record(&rec(
                "m",
                1.0,
                &[("service.name", AttrValue::Str(svc.into()))],
            ))
            .unwrap();
        }
        let mut out = w.drain_all();
        out.sort_by(|x, y| x.value.total_cmp(&y.value));
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].value, 1.0); // b
        assert_eq!(out[1].value, 2.0); // a
        assert_eq!(
            out[1].attrs.get("service.name"),
            Some(&AttrValue::Str("a".into()))
        );
    }

    #[test]
    fn typed_key_does_not_collide_across_value_types() {
        // Int(1) and Str("1") must be different groups (the old string key merged them).
        let mut w = win(AggregateOp::Count, None, FaultAction::Skip, &["k"]);
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Int(1))]))
            .unwrap();
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Str("1".into()))]))
            .unwrap();
        assert_eq!(w.drain_all().len(), 2);
    }

    #[test]
    fn max_groups_sheds_new_groups_and_keeps_existing() {
        // Cap at two groups per window: `a` and `b` fill it, `c` is refused, `a` repeats.
        let mut w = Window::from(WindowConfig {
            group_by: vec!["service.name".into()],
            max_groups: Some(2),
            ..WindowConfig::tumbling(
                Duration::from_nanos(SIZE),
                agg(AggregateOp::Count, None, FaultAction::Skip),
            )
        });
        for svc in ["a", "b", "c", "a"] {
            w.on_record(&rec(
                "m",
                1.0,
                &[("service.name", AttrValue::Str(svc.into()))],
            ))
            .unwrap();
        }
        // `c` would be a third group past the cap: one record shed.
        assert_eq!(w.drain_capped(), 1);
        let mut out = w.drain_all();
        out.sort_by(|x, y| x.value.total_cmp(&y.value));
        assert_eq!(out.len(), 2, "only the two admitted groups emit");
        assert_eq!(out[0].value, 1.0); // b: one record
        assert_eq!(out[1].value, 2.0); // a: still updates past the cap
    }

    // --- on_missing policy ---

    #[test]
    fn on_missing_skip_drops_record_no_silent_fallback() {
        // field `lat` is absent -> record is skipped, NOT folded as `value`.
        let mut w = win(AggregateOp::Count, Some("lat"), FaultAction::Skip, &[]);
        w.on_record(&rec("m", 99.0, &[])).unwrap();
        assert!(
            w.drain_all().is_empty(),
            "skipped record must not form a group"
        );
        assert_eq!(w.drain_skipped(), 1, "the skip is counted");
    }

    #[test]
    fn on_missing_error_fails() {
        let mut w = win(AggregateOp::Avg, Some("lat"), FaultAction::Error, &[]);
        assert!(w.on_record(&rec("m", 1.0, &[])).is_err());
    }

    #[test]
    fn missing_and_invalid_use_separate_policies() {
        // on_missing: skip, on_invalid: error - an absent field is skipped, but a present
        // non-numeric field fails.
        let aggregate = Aggregate {
            op: AggregateOp::Sum,
            field: Some("lat".into()),
            on_missing: FaultAction::Skip,
            on_invalid: FaultAction::Error,
        };
        let mut w = Window::from(WindowConfig::tumbling(
            Duration::from_nanos(SIZE),
            aggregate,
        ));
        w.on_record(&rec("m", 0.0, &[])).unwrap(); // "lat" absent -> skipped
        assert!(
            w.on_record(&rec("m", 0.0, &[("lat", AttrValue::Str("x".into()))]))
                .is_err(),
            "non-numeric `lat` must hit on_invalid=error"
        );
    }

    #[test]
    fn named_numeric_field_is_aggregated() {
        let mut w = win(AggregateOp::Sum, Some("lat"), FaultAction::Error, &[]);
        w.on_record(&rec("m", 0.0, &[("lat", AttrValue::Double(2.5))]))
            .unwrap();
        w.on_record(&rec("m", 0.0, &[("lat", AttrValue::Int(3))]))
            .unwrap();
        assert_eq!(w.drain_all()[0].value, 5.5);
    }

    #[test]
    fn empty_group_yields_no_record() {
        // A group with no numeric samples must not emit +/-INFINITY.
        let a = Agg::new("m".into(), Attrs::new());
        assert_eq!(a.value(AggregateOp::Min), None);
        assert_eq!(a.value(AggregateOp::Avg), None);
        assert_eq!(a.value(AggregateOp::Count), Some(0.0));
    }

    // --- event time and watermarks ---

    #[test]
    fn fires_when_watermark_passes_window_end() {
        let mut w = win(AggregateOp::Count, None, FaultAction::Skip, &[]);
        w.on_record(&rec_at("m", 1.0, 1)).unwrap(); // window [0, SIZE)
        w.on_record(&rec_at("m", 1.0, 500)).unwrap();
        assert!(
            w.drain_ready().is_empty(),
            "watermark 500 < window end {SIZE}, nothing fires yet"
        );
        w.on_record(&rec_at("m", 1.0, SIZE)).unwrap(); // window [SIZE, 2*SIZE); watermark -> SIZE
        let out = w.drain_ready();
        assert_eq!(out.len(), 1, "window [0, SIZE) fires");
        assert_eq!(out[0].value, 2.0, "the two records in [0, SIZE)");
        assert_eq!(out[0].start_ts_nanos, Some(0));
        assert_eq!(out[0].ts_nanos, SIZE);
    }

    #[test]
    fn late_record_is_dropped_and_counted() {
        let mut w = win(AggregateOp::Count, None, FaultAction::Skip, &[]);
        w.on_record(&rec_at("m", 1.0, 100)).unwrap(); // [0, SIZE)
        w.on_record(&rec_at("m", 1.0, SIZE + 500)).unwrap(); // watermark -> SIZE+500
        assert_eq!(w.drain_ready().len(), 1, "[0, SIZE) fires");
        // A record for the already-fired window arrives late.
        w.on_record(&rec_at("m", 1.0, 200)).unwrap();
        assert_eq!(w.drain_late(), 1, "the late record is counted");
        assert!(w.drain_ready().is_empty());
    }

    #[test]
    fn allowed_lateness_delays_firing_and_includes_late_records() {
        // watermark = max_event - 500, so [0, SIZE) fires only once max_event >= SIZE+500.
        let mut w = Window::from(WindowConfig {
            lateness: Duration::from_nanos(500),
            ..WindowConfig::tumbling(
                Duration::from_nanos(SIZE),
                agg(AggregateOp::Count, None, FaultAction::Skip),
            )
        });
        w.on_record(&rec_at("m", 1.0, 100)).unwrap(); // [0, SIZE)
        w.on_record(&rec_at("m", 1.0, 200)).unwrap(); // [0, SIZE)
        w.on_record(&rec_at("m", 1.0, SIZE + 200)).unwrap(); // watermark SIZE-300 < SIZE
        assert!(w.drain_ready().is_empty(), "grace not elapsed");
        // Out-of-order record still lands in [0, SIZE) because the grace keeps it open.
        w.on_record(&rec_at("m", 1.0, 300)).unwrap();
        w.on_record(&rec_at("m", 1.0, SIZE + 500)).unwrap(); // watermark SIZE -> fires
        let out = w.drain_ready();
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].value, 3.0,
            "the three records in [0, SIZE), including the out-of-order one"
        );
    }

    #[test]
    fn name_override_renames_the_output() {
        let mut w = Window::from(WindowConfig {
            name: Some("renamed".to_string()),
            ..WindowConfig::tumbling(
                Duration::from_nanos(SIZE),
                agg(AggregateOp::Count, None, FaultAction::Skip),
            )
        });
        w.on_record(&rec("m", 1.0, &[])).unwrap();
        assert_eq!(w.drain_all()[0].name, "renamed");
    }

    // --- sliding windows ---

    #[test]
    fn sliding_folds_a_record_into_overlapping_windows() {
        // size 1000, slide 500: t=700 falls in both [0,1000) and [500,1500).
        let mut w = Window::from(WindowConfig {
            slide: Duration::from_nanos(500),
            ..WindowConfig::tumbling(
                Duration::from_nanos(1000),
                agg(AggregateOp::Count, None, FaultAction::Skip),
            )
        });
        w.on_record(&rec_at("m", 1.0, 700)).unwrap();
        let mut out = w.drain_all();
        out.sort_by_key(|r| r.start_ts_nanos);
        assert_eq!(out.len(), 2, "one record, two overlapping windows");
        assert_eq!((out[0].start_ts_nanos, out[0].ts_nanos), (Some(0), 1000));
        assert_eq!((out[1].start_ts_nanos, out[1].ts_nanos), (Some(500), 1500));
        assert!(out.iter().all(|r| r.value == 1.0));
    }

    #[test]
    fn sliding_windows_fire_as_the_watermark_advances() {
        let mut w = Window::from(WindowConfig {
            slide: Duration::from_nanos(500),
            ..WindowConfig::tumbling(
                Duration::from_nanos(1000),
                agg(AggregateOp::Count, None, FaultAction::Skip),
            )
        });
        w.on_record(&rec_at("m", 1.0, 200)).unwrap(); // [0,1000)
        w.on_record(&rec_at("m", 1.0, 700)).unwrap(); // [0,1000) and [500,1500)
        assert!(w.drain_ready().is_empty(), "watermark 700 < any window end");
        w.on_record(&rec_at("m", 1.0, 1100)).unwrap(); // watermark -> 1100
        let out = w.drain_ready();
        assert_eq!(out.len(), 1, "only [0,1000) has closed");
        assert_eq!(out[0].start_ts_nanos, Some(0));
        assert_eq!(out[0].value, 2.0, "the records at 200 and 700");
    }

    // --- state inspection ---

    #[test]
    fn snapshot_reports_open_groups_with_labels_and_samples() {
        let mut w = win(AggregateOp::Sum, None, FaultAction::Skip, &["svc"]);
        w.on_record(&rec("m", 2.0, &[("svc", AttrValue::Str("a".into()))]))
            .unwrap();
        w.on_record(&rec("m", 5.0, &[("svc", AttrValue::Str("a".into()))]))
            .unwrap();
        w.on_record(&rec("m", 9.0, &[("svc", AttrValue::Str("b".into()))]))
            .unwrap();

        let snap = w.snapshot();
        assert_eq!(snap.kind, "window");
        let mut groups = snap.groups;
        groups.sort_by(|x, y| x.labels["svc"].cmp(&y.labels["svc"]));
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].labels["svc"], "a");
        assert_eq!(groups[0].value, Some(7.0), "sum of a's two samples");
        assert_eq!(groups[0].samples, 2);
        assert!(groups[0].inputs.is_empty(), "inputs are a join concern");
        assert_eq!((groups[0].start_nanos, groups[0].end_nanos), (0, SIZE));
        assert_eq!(groups[1].labels["svc"], "b");
        assert_eq!(groups[1].value, Some(9.0));
        assert_eq!(groups[1].samples, 1);

        // Only open windows are reported: once drained, the snapshot is empty.
        let _ = w.drain_all();
        assert!(w.snapshot().groups.is_empty());
    }

    // --- helpers ---

    fn agg(op: AggregateOp, field: Option<&str>, on_missing: FaultAction) -> Aggregate {
        Aggregate {
            op,
            field: field.map(str::to_string),
            on_missing,
            on_invalid: on_missing,
        }
    }

    fn win(
        op: AggregateOp,
        field: Option<&str>,
        on_missing: FaultAction,
        group_by: &[&str],
    ) -> Window {
        Window::from(WindowConfig {
            group_by: group_by.iter().map(|s| s.to_string()).collect(),
            ..WindowConfig::tumbling(Duration::from_nanos(SIZE), agg(op, field, on_missing))
        })
    }

    fn rec(name: &str, value: f64, attrs: &[(&str, AttrValue)]) -> Record {
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: name.into(),
            value,
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    fn rec_at(name: &str, value: f64, ts: u64) -> Record {
        Record {
            ts_nanos: ts,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: name.into(),
            value,
            attrs: Attrs::new(),
        }
    }

    fn one_group(op: AggregateOp, values: &[f64]) -> f64 {
        let mut w = win(op, None, FaultAction::Skip, &[]);
        for &v in values {
            w.on_record(&rec("m", v, &[])).unwrap();
        }
        let out = w.drain_all();
        assert_eq!(out.len(), 1);
        out[0].value
    }
}
