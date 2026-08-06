//! `window`: group records by `group_by` and reduce each group over a tumbling time
//! window. Windows are placed in *event time* and fired on a watermark. Stateful.
//!
//! [`Window`] is the pure, synchronous core (fold + fire); [`run`] is the async driver
//! that owns only I/O.

use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use crate::record::{AttrValue, Attrs, Record};
use anyhow::{Result, bail};
use headrace_ir::{Aggregate, AggregateOp, OnMissing};
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
    lateness_nanos: u64,
    group_by: Vec<String>,
    aggregate: Aggregate,
    /// Open windows keyed by start; each holds its per-group accumulators. Ordered so
    /// the earliest-ready windows fire first.
    windows: BTreeMap<u64, HashMap<GroupKey, Agg>>,
    max_event: u64,
    skipped: u64,
    late: u64,
}

impl Window {
    pub fn new(
        size_nanos: u64,
        lateness_nanos: u64,
        group_by: Vec<String>,
        aggregate: Aggregate,
    ) -> Self {
        Self {
            size_nanos,
            lateness_nanos,
            group_by,
            aggregate,
            windows: BTreeMap::new(),
            max_event: 0,
            skipped: 0,
            late: 0,
        }
    }

    /// Event time up to which input is treated as complete: the newest event seen, less
    /// the allowed lateness.
    fn watermark(&self) -> u64 {
        self.max_event.saturating_sub(self.lateness_nanos)
    }

    /// Start of the tumbling window containing event time `t`.
    fn window_start(&self, t: u64) -> u64 {
        t - (t % self.size_nanos)
    }

    /// Fold one record into its event-time window. A record whose window has already
    /// fired is counted late and dropped. `Err` only under `OnMissing::Error`.
    pub fn on_record(&mut self, rec: &Record) -> Result<()> {
        let start = self.window_start(rec.ts_nanos);
        if start + self.size_nanos <= self.watermark() {
            self.late += 1;
            return Ok(());
        }
        let v = match value_of(rec, &self.aggregate) {
            Some(v) => v,
            None => match self.aggregate.on_missing {
                OnMissing::Skip => {
                    self.skipped += 1;
                    // A skipped record still advances the stream's event time.
                    self.max_event = self.max_event.max(rec.ts_nanos);
                    return Ok(());
                }
                OnMissing::Error => bail!(
                    "record missing numeric field `{}`",
                    self.aggregate.field.as_deref().unwrap_or("value")
                ),
            },
        };
        let (key, attrs) = group_key(rec, &self.group_by);
        self.windows
            .entry(start)
            .or_default()
            .entry(key)
            .or_insert_with(|| Agg::new(rec.name.clone(), attrs))
            .add(v);
        self.max_event = self.max_event.max(rec.ts_nanos);
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
                    name: a.name,
                    value,
                    attrs: a.attrs,
                });
            }
        }
        out
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

/// The numeric sample for a record, or `None` if the configured field is
/// absent or non-numeric. No silent fallback to `value` - that is the caller's
/// `on_missing` policy to decide.
fn value_of(rec: &Record, agg: &Aggregate) -> Option<f64> {
    match agg.field.as_deref() {
        None | Some("value") => Some(rec.value),
        Some(f) => rec.lookup(f).and_then(AttrValue::as_f64),
    }
}

/// The window transform's settings, parsed from the IR node.
pub(super) struct Spec {
    pub size: String,
    pub allowed_lateness: Option<String>,
    pub idle_timeout: Option<String>,
    pub group_by: Vec<String>,
    pub aggregate: Aggregate,
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
) -> Result<()> {
    let Spec {
        size,
        allowed_lateness,
        idle_timeout,
        group_by,
        aggregate,
    } = spec;
    let size_nanos = humantime::parse_duration(&size)?.as_nanos() as u64;
    let lateness_nanos = match &allowed_lateness {
        Some(l) => humantime::parse_duration(l)?.as_nanos() as u64,
        None => 0,
    };
    let idle = match &idle_timeout {
        Some(t) => Some(humantime::parse_duration(t)?),
        None => None,
    };
    let mut win = Window::new(size_nanos, lateness_nanos, group_by, aggregate);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(rec) => {
                    win.on_record(&rec)?;
                    meter_drops(&mut win, &nm);
                    if !emit(win.drain_ready(), tx.as_ref(), &nm).await {
                        return Ok(());
                    }
                }
                None => break,
            },
            // Fires only when `idle` is set; otherwise this branch never completes.
            _ = maybe_sleep(idle) => {
                if !emit(win.drain_all(), tx.as_ref(), &nm).await {
                    return Ok(());
                }
            }
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
}

/// Forward fired records downstream. Returns `false` if the downstream is gone.
async fn emit(recs: Vec<Record>, tx: &dyn Producer, nm: &NodeMetrics) -> bool {
    if recs.is_empty() {
        return true;
    }
    nm.window_flushed(recs.len() as u64);
    for rec in recs {
        nm.out();
        if tx.send(None, rec).await.is_err() {
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
        let mut w = win(AggregateOp::Sum, None, OnMissing::Skip, &[]);
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
        let mut w = win(AggregateOp::Count, None, OnMissing::Skip, &["service.name"]);
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
        let mut w = win(AggregateOp::Count, None, OnMissing::Skip, &["k"]);
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Int(1))]))
            .unwrap();
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Str("1".into()))]))
            .unwrap();
        assert_eq!(w.drain_all().len(), 2);
    }

    // --- on_missing policy ---

    #[test]
    fn on_missing_skip_drops_record_no_silent_fallback() {
        // field `lat` is absent -> record is skipped, NOT folded as `value`.
        let mut w = win(AggregateOp::Count, Some("lat"), OnMissing::Skip, &[]);
        w.on_record(&rec("m", 99.0, &[])).unwrap();
        assert!(
            w.drain_all().is_empty(),
            "skipped record must not form a group"
        );
        assert_eq!(w.drain_skipped(), 1, "the skip is counted");
    }

    #[test]
    fn on_missing_error_fails() {
        let mut w = win(AggregateOp::Avg, Some("lat"), OnMissing::Error, &[]);
        assert!(w.on_record(&rec("m", 1.0, &[])).is_err());
    }

    #[test]
    fn named_numeric_field_is_aggregated() {
        let mut w = win(AggregateOp::Sum, Some("lat"), OnMissing::Error, &[]);
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
        let mut w = win(AggregateOp::Count, None, OnMissing::Skip, &[]);
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
        let mut w = win(AggregateOp::Count, None, OnMissing::Skip, &[]);
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
        let mut w = Window::new(
            SIZE,
            500,
            vec![],
            agg(AggregateOp::Count, None, OnMissing::Skip),
        );
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

    // --- helpers ---

    fn agg(op: AggregateOp, field: Option<&str>, on_missing: OnMissing) -> Aggregate {
        Aggregate {
            op,
            field: field.map(str::to_string),
            on_missing,
        }
    }

    fn win(
        op: AggregateOp,
        field: Option<&str>,
        on_missing: OnMissing,
        group_by: &[&str],
    ) -> Window {
        Window::new(
            SIZE,
            0,
            group_by.iter().map(|s| s.to_string()).collect(),
            agg(op, field, on_missing),
        )
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
        let mut w = win(op, None, OnMissing::Skip, &[]);
        for &v in values {
            w.on_record(&rec("m", v, &[])).unwrap();
        }
        let out = w.drain_all();
        assert_eq!(out.len(), 1);
        out[0].value
    }
}
