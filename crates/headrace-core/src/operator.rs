use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use crate::record::{AttrValue, Attrs, Record, now_nanos};
use anyhow::{Result, bail};
use headrace_ir::{Aggregate, AggregateOp, OnMissing, Operator};
use std::collections::HashMap;

pub async fn run(
    op: Operator,
    rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    match op {
        Operator::Filter { key, equals, .. } => filter(key, equals, rx, tx, nm).await,
        Operator::Window {
            size,
            group_by,
            aggregate,
            ..
        } => window(size, group_by, aggregate, rx, tx, nm).await,
        // Forward-compat: an IR node type this build does not implement.
        other => bail!("unsupported operator `{}`", other.id()),
    }
}

// ---- filter ----

/// Keep predicate, extracted so it can be tested without a channel.
fn keep(rec: &Record, key: &str, equals: &Option<String>) -> bool {
    match (rec.lookup(key), equals) {
        (Some(v), Some(want)) => v.to_string() == *want,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

async fn filter(
    key: String,
    equals: Option<String>,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    while let Some(rec) = rx.recv().await {
        if keep(&rec, &key, &equals) {
            nm.out();
            if tx.send(None, rec).await.is_err() {
                break;
            }
        } else {
            nm.dropped(1);
        }
    }
    Ok(())
}

// ---- window ----

/// A key part per `group_by` dimension, in the operator's declared order.
///
/// Canonical and hashable: types stay distinct (`Int(1)` ≠ `Str("1")`) and a
/// missing attribute (`Absent`) differs from an empty string, so distinct groups
/// never collide. This is also the identity that partitions state across workers
/// in the scaled deployment (DESIGN.md: group_key → partition → worker-local state).
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
    /// group would otherwise emit `±INFINITY`. Count of an empty group is `0`.
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

/// The stateful core of the window operator: pure, synchronous, testable in isolation.
/// The async driver below owns only timing and I/O.
pub struct Window {
    group_by: Vec<String>,
    aggregate: Aggregate,
    groups: HashMap<GroupKey, Agg>,
    skipped: u64,
}

impl Window {
    pub fn new(group_by: Vec<String>, aggregate: Aggregate) -> Self {
        Self {
            group_by,
            aggregate,
            groups: HashMap::new(),
            skipped: 0,
        }
    }

    /// Fold one record into its group. `Err` only under `OnMissing::Error`.
    pub fn on_record(&mut self, rec: &Record) -> Result<()> {
        let v = match value_of(rec, &self.aggregate) {
            Some(v) => v,
            None => match self.aggregate.on_missing {
                OnMissing::Skip => {
                    self.skipped += 1;
                    return Ok(());
                }
                OnMissing::Error => bail!(
                    "record missing numeric field `{}`",
                    self.aggregate.field.as_deref().unwrap_or("value")
                ),
            },
        };
        let (key, attrs) = group_key(rec, &self.group_by);
        self.groups
            .entry(key)
            .or_insert_with(|| Agg::new(rec.name.clone(), attrs))
            .add(v);
        Ok(())
    }

    /// Records skipped since the last call (drained). The driver logs/meters these;
    /// keeping it out of `flush` leaves the reduce path pure.
    pub fn drain_skipped(&mut self) -> u64 {
        std::mem::take(&mut self.skipped)
    }

    /// Emit one record per non-empty group for the window `[start, end)`, and reset.
    pub fn flush(&mut self, start_nanos: u64, end_nanos: u64) -> Vec<Record> {
        let op = self.aggregate.op;
        self.groups
            .drain()
            .filter_map(|(_, a)| {
                a.value(op).map(|value| Record {
                    ts_nanos: end_nanos,
                    start_ts_nanos: Some(start_nanos),
                    resource: Attrs::new(),
                    scope: None,
                    name: a.name.clone(),
                    value,
                    attrs: a.attrs.clone(),
                })
            })
            .collect()
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
/// absent or non-numeric. No silent fallback to `value` — that is the caller's
/// `on_missing` policy to decide.
fn value_of(rec: &Record, agg: &Aggregate) -> Option<f64> {
    match agg.field.as_deref() {
        None | Some("value") => Some(rec.value),
        Some(f) => rec.lookup(f).and_then(AttrValue::as_f64),
    }
}

async fn window(
    size: String,
    group_by: Vec<String>,
    aggregate: Aggregate,
    mut rx: Box<dyn Consumer>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
    let period = humantime::parse_duration(&size)?;
    let period_nanos = period.as_nanos() as u64;
    let mut ticker = tokio::time::interval(period);
    ticker.tick().await; // drop the immediate first tick
    let mut win = Window::new(group_by, aggregate);

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(rec) => win.on_record(&rec)?,
                None => break,
            },
            _ = ticker.tick() => {
                if !emit(&mut win, now_nanos(), period_nanos, tx.as_ref(), &nm).await {
                    return Ok(());
                }
            }
        }
    }
    // Best-effort final flush when the upstream closes cleanly.
    emit(&mut win, now_nanos(), period_nanos, tx.as_ref(), &nm).await;
    Ok(())
}

/// Flush the window and forward its records. Returns `false` if the downstream is gone.
async fn emit(
    win: &mut Window,
    end: u64,
    period_nanos: u64,
    tx: &dyn Producer,
    nm: &NodeMetrics,
) -> bool {
    let skipped = win.drain_skipped();
    if skipped > 0 {
        tracing::warn!(
            skipped,
            "window: dropped records with missing/non-numeric field"
        );
        nm.dropped(skipped);
    }
    let start = end.saturating_sub(period_nanos);
    let recs = win.flush(start, end);
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

    fn agg(op: AggregateOp, field: Option<&str>, on_missing: OnMissing) -> Aggregate {
        Aggregate {
            op,
            field: field.map(str::to_string),
            on_missing,
        }
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

    // --- filter ---

    #[test]
    fn keep_semantics() {
        let r = rec(
            "m",
            1.0,
            &[("service.name", AttrValue::Str("checkout".into()))],
        );
        assert!(keep(&r, "service.name", &None)); // key exists
        assert!(keep(&r, "service.name", &Some("checkout".into()))); // equals
        assert!(!keep(&r, "service.name", &Some("cart".into()))); // differs
        assert!(!keep(&r, "missing", &None)); // absent key
    }

    #[test]
    fn keep_equals_is_stringwise_across_types() {
        let r = rec("m", 1.0, &[("http.status", AttrValue::Int(200))]);
        assert!(keep(&r, "http.status", &Some("200".into())));
    }

    // --- aggregate math ---

    fn one_group(op: AggregateOp, values: &[f64]) -> f64 {
        let mut w = Window::new(vec![], agg(op, None, OnMissing::Skip));
        for &v in values {
            w.on_record(&rec("m", v, &[])).unwrap();
        }
        let out = w.flush(0, 100);
        assert_eq!(out.len(), 1);
        out[0].value
    }

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
    fn flush_emits_window_bounds_and_resets() {
        let mut w = Window::new(vec![], agg(AggregateOp::Sum, None, OnMissing::Skip));
        w.on_record(&rec("m", 1.0, &[])).unwrap();
        let out = w.flush(50, 100);
        assert_eq!(out[0].start_ts_nanos, Some(50));
        assert_eq!(out[0].ts_nanos, 100);
        // Drained: a second flush with no input emits nothing.
        assert!(w.flush(100, 150).is_empty());
    }

    // --- grouping ---

    #[test]
    fn groups_split_by_key_and_carry_attrs() {
        let mut w = Window::new(
            vec!["service.name".into()],
            agg(AggregateOp::Count, None, OnMissing::Skip),
        );
        for svc in ["a", "a", "b"] {
            w.on_record(&rec(
                "m",
                1.0,
                &[("service.name", AttrValue::Str(svc.into()))],
            ))
            .unwrap();
        }
        let mut out = w.flush(0, 1);
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
        let mut w = Window::new(
            vec!["k".into()],
            agg(AggregateOp::Count, None, OnMissing::Skip),
        );
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Int(1))]))
            .unwrap();
        w.on_record(&rec("m", 1.0, &[("k", AttrValue::Str("1".into()))]))
            .unwrap();
        assert_eq!(w.flush(0, 1).len(), 2);
    }

    // --- on_missing policy ---

    #[test]
    fn on_missing_skip_drops_record_no_silent_fallback() {
        // field `lat` is absent → record is skipped, NOT folded as `value`.
        let mut w = Window::new(
            vec![],
            agg(AggregateOp::Count, Some("lat"), OnMissing::Skip),
        );
        w.on_record(&rec("m", 99.0, &[])).unwrap();
        assert!(
            w.flush(0, 1).is_empty(),
            "skipped record must not form a group"
        );
    }

    #[test]
    fn on_missing_error_fails() {
        let mut w = Window::new(vec![], agg(AggregateOp::Avg, Some("lat"), OnMissing::Error));
        assert!(w.on_record(&rec("m", 1.0, &[])).is_err());
    }

    #[test]
    fn named_numeric_field_is_aggregated() {
        let mut w = Window::new(vec![], agg(AggregateOp::Sum, Some("lat"), OnMissing::Error));
        w.on_record(&rec("m", 0.0, &[("lat", AttrValue::Double(2.5))]))
            .unwrap();
        w.on_record(&rec("m", 0.0, &[("lat", AttrValue::Int(3))]))
            .unwrap();
        assert_eq!(w.flush(0, 1)[0].value, 5.5);
    }

    #[test]
    fn empty_group_yields_no_record() {
        // A group with no numeric samples must not emit ±INFINITY.
        let a = Agg::new("m".into(), Attrs::new());
        assert_eq!(a.value(AggregateOp::Min), None);
        assert_eq!(a.value(AggregateOp::Avg), None);
        assert_eq!(a.value(AggregateOp::Count), Some(0.0));
    }
}

/// Exercises the async operators against mocked `Backend` handles — the seam a
/// networked backend swaps into. Run with `--features mocks`.
#[cfg(all(test, feature = "mocks"))]
mod seam_tests {
    use super::*;
    use crate::backend::{MockConsumer, MockProducer};
    use crate::metrics::{Metrics, NodeKind, NodeRecorder, SharedMetrics};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct Counts {
        out: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    }
    impl Metrics for Counts {
        fn node(&self, _: &str, _: NodeKind) -> Arc<dyn NodeRecorder> {
            Arc::new(CountRecorder {
                out: self.out.clone(),
                dropped: self.dropped.clone(),
            })
        }
    }
    struct CountRecorder {
        out: Arc<AtomicU64>,
        dropped: Arc<AtomicU64>,
    }
    impl NodeRecorder for CountRecorder {
        fn record_out(&self) {
            self.out.fetch_add(1, Ordering::Relaxed);
        }
        fn record_dropped(&self, n: u64) {
            self.dropped.fetch_add(n, Ordering::Relaxed);
        }
        fn window_flushed(&self, _: u64) {}
        fn node_error(&self) {}
    }

    fn svc_rec(svc: &str) -> Record {
        Record {
            ts_nanos: 1,
            start_ts_nanos: None,
            resource: Attrs::new(),
            scope: None,
            name: "m".into(),
            value: 1.0,
            attrs: [("service.name".to_string(), AttrValue::Str(svc.into()))]
                .into_iter()
                .collect(),
        }
    }

    #[tokio::test]
    async fn filter_forwards_matching_records_and_meters_drops() {
        // Consumer yields checkout, then cart, then closes.
        let mut rx = MockConsumer::new();
        let mut seq = mockall::Sequence::new();
        rx.expect_recv()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Some(svc_rec("checkout")));
        rx.expect_recv()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Some(svc_rec("cart")));
        rx.expect_recv()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| None);

        // Producer must receive exactly the checkout record, unkeyed (in-process).
        let mut tx = MockProducer::new();
        tx.expect_send()
            .times(1)
            .withf(|key, rec| {
                key.is_none()
                    && rec.lookup("service.name").map(|v| v.to_string()) == Some("checkout".into())
            })
            .returning(|_, _| Ok(()));

        let counts = Counts::default();
        let (out, dropped) = (counts.out.clone(), counts.dropped.clone());
        let m: SharedMetrics = Arc::new(counts);
        let nm = NodeMetrics::bind(&m, "f", NodeKind::Filter);

        filter(
            "service.name".into(),
            Some("checkout".into()),
            Box::new(rx),
            Box::new(tx),
            nm,
        )
        .await
        .unwrap();

        assert_eq!(out.load(Ordering::Relaxed), 1, "one forwarded");
        assert_eq!(dropped.load(Ordering::Relaxed), 1, "one dropped");
    }
}
