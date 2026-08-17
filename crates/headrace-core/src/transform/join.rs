//! `join`: align several windowed inputs on their shared labels and window, and emit one
//! record per aligned `(labels, window)`. With a `value` expression it reduces the inputs
//! inline (`a - b`); otherwise it carries each input's value as an attribute named by
//! input id, for a downstream `map`/`wasm` to reduce. See ADR-0012.
//!
//! Inputs are windowed at the same size, so their epoch-aligned `[start, end)` bounds
//! coincide; a record's own attributes are exactly the `group_by` labels, so the
//! alignment key is `(attrs, start, end)`. A bucket fires when every input has supplied a
//! value; an incomplete bucket is evicted once every input has advanced past its window
//! (the join watermark, `min` of each input's newest window end).

use super::expr::Expr;
use crate::backend::{Consumer, Producer};
use crate::inspect::{GroupSnapshot, Inspect, NodeSnapshot, labels_of, publish, recv_query};
use crate::metrics::{DropReason, NodeMetrics};
use crate::record::{AttrValue, Attrs, Record};
use anyhow::{Result, anyhow};
use std::collections::{BTreeMap, HashMap};

struct Bucket {
    start: u64,
    end: u64,
    /// The shared `group_by` labels for this aligned key.
    labels: Attrs,
    /// One slot per input, filled as each input's value arrives.
    values: Vec<Option<f64>>,
    filled: usize,
}

/// The join transform's settings, parsed from the IR node.
pub(super) struct Spec {
    pub id: String,
    pub inputs: Vec<String>,
    pub name: Option<String>,
    pub value: Option<String>,
    pub max_groups: Option<usize>,
}

pub(super) async fn run(
    spec: Spec,
    rxs: Vec<Box<dyn Consumer>>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
    mut inspect: Option<Inspect>,
) -> Result<()> {
    let Spec {
        id,
        inputs,
        name,
        value,
        max_groups,
    } = spec;
    let expr = match &value {
        Some(v) => Some(Expr::parse(v).map_err(|e| anyhow!("invalid join expression: {}", e.0))?),
        None => None,
    };
    let out_name = name.unwrap_or(id);
    let n = rxs.len();

    // Merge the N inputs into one tagged stream: a forwarder per input tags records with
    // its index. The merged channel closes once every forwarder ends (inputs drained).
    let (merged_tx, mut merged_rx) = tokio::sync::mpsc::channel::<(usize, Record)>(1024);
    for (i, mut rx) in rxs.into_iter().enumerate() {
        let merged_tx = merged_tx.clone();
        tokio::spawn(async move {
            while let Some(rec) = rx.recv().await {
                if merged_tx.send((i, rec)).await.is_err() {
                    break;
                }
            }
        });
    }
    drop(merged_tx);

    let mut buckets: HashMap<Vec<u8>, Bucket> = HashMap::new();
    let mut max_end = vec![0u64; n];

    loop {
        let (i, rec) = tokio::select! {
            maybe = merged_rx.recv() => match maybe {
                Some(item) => item,
                None => break,
            },
            // Answer an inspect query from the node's own loop, so the snapshot is
            // consistent with the buckets folded above.
            query = recv_query(&mut inspect) => {
                match query {
                    Some(reply) => { let _ = reply.send(snapshot(&buckets, &inputs)); }
                    None => inspect = None, // every Handle dropped; stop polling
                }
                continue;
            }
        };
        let start = rec.start_ts_nanos.unwrap_or(rec.ts_nanos);
        let end = rec.ts_nanos;
        max_end[i] = max_end[i].max(end);

        let key = bucket_key(start, end, &rec.attrs);
        // An existing bucket always takes the record; a new one is refused once the node is
        // at max_groups, shedding rather than growing buckets without bound.
        if !buckets.contains_key(&key) && max_groups.is_some_and(|c| buckets.len() >= c) {
            nm.dropped(1, DropReason::Capped);
        } else {
            let bucket = buckets.entry(key.clone()).or_insert_with(|| Bucket {
                start,
                end,
                labels: rec.attrs.clone(),
                values: vec![None; n],
                filled: 0,
            });
            if bucket.values[i].is_none() {
                bucket.filled += 1;
            }
            bucket.values[i] = Some(rec.value);

            if bucket.filled == n {
                let bucket = buckets.remove(&key).expect("bucket just updated");
                match emit(&out_name, &inputs, expr.as_ref(), bucket) {
                    Some(record) => {
                        if tx.send(record).await.is_err() {
                            return Ok(());
                        }
                        nm.out();
                    }
                    // the reduce expression could not be evaluated
                    None => nm.dropped(1, DropReason::Invalid),
                }
            }
        }

        // Evict incomplete buckets every input has now advanced past.
        let watermark = max_end.iter().copied().min().unwrap_or(0);
        let stale: Vec<Vec<u8>> = buckets
            .iter()
            .filter(|(_, b)| b.end <= watermark)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            buckets.remove(&k);
            nm.dropped(1, DropReason::Incomplete);
        }

        publish(&inspect, || snapshot(&buckets, &inputs));
    }
    Ok(())
}

/// A read-only view of the open (incomplete, not-yet-fired) buckets, for state inspection
/// (ADR-0014). Each reports the per-input values filled so far; a join bucket has no single
/// `value` until it completes and fires, and `samples` is how many of its inputs have arrived.
fn snapshot(buckets: &HashMap<Vec<u8>, Bucket>, inputs: &[String]) -> NodeSnapshot {
    let mut groups = Vec::new();
    for b in buckets.values() {
        let mut filled = BTreeMap::new();
        for (id, v) in inputs.iter().zip(&b.values) {
            if let Some(v) = v {
                filled.insert(id.clone(), *v);
            }
        }
        groups.push(GroupSnapshot {
            labels: labels_of(&b.labels),
            start_nanos: b.start,
            end_nanos: b.end,
            value: None,
            inputs: filled,
            samples: b.filled as u64,
        });
    }
    NodeSnapshot {
        kind: "join",
        groups,
    }
}

/// Build the output record for a complete bucket. With an expression, evaluate it against
/// the per-input values (exposed as attributes by id) and emit a clean record (labels
/// only); without one, carry the per-input values as attributes. `None` if the expression
/// could not be evaluated.
fn emit(name: &str, inputs: &[String], expr: Option<&Expr>, bucket: Bucket) -> Option<Record> {
    let value = match expr {
        Some(expr) => {
            let mut probe = bucket.labels.clone();
            carry_inputs(&mut probe, inputs, &bucket.values);
            let record = record(name, bucket.start, bucket.end, 0.0, probe);
            match expr.eval(&record) {
                Ok(v) if v.is_finite() => v,
                _ => return None,
            }
        }
        None => 0.0,
    };
    let mut attrs = bucket.labels;
    if expr.is_none() {
        carry_inputs(&mut attrs, inputs, &bucket.values);
    }
    Some(record(name, bucket.start, bucket.end, value, attrs))
}

/// Add each input's value to `attrs` under the input's node id.
fn carry_inputs(attrs: &mut Attrs, inputs: &[String], values: &[Option<f64>]) {
    for (id, v) in inputs.iter().zip(values) {
        attrs.insert(id.clone(), AttrValue::Double(v.expect("bucket complete")));
    }
}

fn record(name: &str, start: u64, end: u64, value: f64, attrs: Attrs) -> Record {
    Record {
        ts_nanos: end,
        start_ts_nanos: Some(start),
        resource: Attrs::new(),
        scope: None,
        name: name.to_string(),
        value,
        attrs,
    }
}

/// A stable key for an aligned bucket: window bounds plus the labels. Typed and
/// length-prefixed (not a formatted string), so two labels that stringify alike but differ
/// in type - `Int(1)` vs `Str("1")` - never collapse into one bucket. `labels` is a
/// `BTreeMap`, so iteration order is stable across records.
fn bucket_key(start: u64, end: u64, labels: &Attrs) -> Vec<u8> {
    let mut k = Vec::new();
    k.extend_from_slice(&start.to_le_bytes());
    k.extend_from_slice(&end.to_le_bytes());
    for (name, v) in labels {
        k.extend_from_slice(&(name.len() as u64).to_le_bytes());
        k.extend_from_slice(name.as_bytes());
        v.write_key_bytes(&mut k);
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, InProcess, KeySpec};
    use crate::metrics::{NodeKind, NodeMetrics};
    use crate::{NoopMetrics, SharedMetrics};
    use std::sync::Arc;

    #[test]
    fn bucket_key_is_typed_and_stable() {
        let label = |v: AttrValue| {
            let mut a = Attrs::new();
            a.insert("code".into(), v);
            a
        };
        let int = label(AttrValue::Int(1));
        let string = label(AttrValue::Str("1".into()));
        // Same label name, values that stringify alike but differ in type: distinct buckets,
        // so two inputs disagreeing on a label's type never align into one.
        assert_ne!(bucket_key(0, 60, &int), bucket_key(0, 60, &string));
        // Identical (start, end, labels) is a stable, equal key.
        assert_eq!(bucket_key(0, 60, &int), bucket_key(0, 60, &int));
        // A different window is a different bucket.
        assert_ne!(bucket_key(0, 60, &int), bucket_key(60, 120, &int));
    }

    #[tokio::test]
    async fn reduces_aligned_inputs() {
        let (feeders, mut out) = setup(&["a", "b"], Some("a - b"));
        feeders[0]
            .send(wrec("checkout", 0, 60, 214.0))
            .await
            .unwrap();
        feeders[1]
            .send(wrec("checkout", 0, 60, 190.0))
            .await
            .unwrap();
        let got = out.recv().await.expect("joined record");
        assert_eq!(got.value, 24.0);
        assert_eq!(
            got.attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
        assert!(
            !got.attrs.contains_key("a"),
            "the reduce drops per-input attrs"
        );
        drop(feeders);
    }

    #[tokio::test]
    async fn align_only_carries_inputs_as_attributes() {
        let (feeders, mut out) = setup(&["a", "b"], None);
        feeders[0].send(wrec("checkout", 0, 60, 5.0)).await.unwrap();
        feeders[1].send(wrec("checkout", 0, 60, 8.0)).await.unwrap();
        let got = out.recv().await.expect("joined record");
        assert_eq!(got.attrs.get("a"), Some(&AttrValue::Double(5.0)));
        assert_eq!(got.attrs.get("b"), Some(&AttrValue::Double(8.0)));
        drop(feeders);
    }

    #[tokio::test]
    async fn drops_unaligned_windows() {
        let (feeders, mut out) = setup(&["a", "b"], Some("a + b"));
        // `a` has window [0,60); `b` jumps to [60,120), so [0,60) never completes.
        feeders[0].send(wrec("checkout", 0, 60, 1.0)).await.unwrap();
        feeders[1]
            .send(wrec("checkout", 60, 120, 2.0))
            .await
            .unwrap();
        // `a` advances to [60,120) too, completing it; [0,60) is evicted, never emitted.
        feeders[0]
            .send(wrec("checkout", 60, 120, 3.0))
            .await
            .unwrap();
        let got = out.recv().await.expect("the [60,120) join");
        assert_eq!(got.value, 5.0);
        assert_eq!(got.start_ts_nanos, Some(60));
        drop(feeders);
    }

    #[tokio::test]
    async fn drops_when_the_reduce_cannot_evaluate() {
        // `c` is not an input, so the expression can't resolve and nothing is emitted.
        let (feeders, mut out) = setup(&["a", "b"], Some("a - c"));
        feeders[0].send(wrec("checkout", 0, 60, 5.0)).await.unwrap();
        feeders[1].send(wrec("checkout", 0, 60, 8.0)).await.unwrap();
        drop(feeders); // the [0,60) bucket completes, the reduce fails, nothing forwards
        assert!(out.recv().await.is_none());
    }

    #[tokio::test]
    async fn snapshot_reports_incomplete_buckets() {
        let mut be = InProcess::new(64);
        let fa = be.producer("a", &KeySpec::Unkeyed);
        let fb = be.producer("b", &KeySpec::Unkeyed);
        let rxs = vec![be.consumer("a"), be.consumer("b")];
        let tx = be.producer("out", &KeySpec::Unkeyed);
        let mut out = be.consumer("out");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "j", NodeKind::Join);
        let (inspect, handle, _events) = Inspect::channel();
        let ids = vec!["a".to_string(), "b".to_string()];
        let spec = Spec {
            id: "j".into(),
            inputs: ids,
            name: None,
            value: Some("a + b".into()),
            max_groups: None,
        };
        tokio::spawn(run(spec, rxs, tx, nm, Some(inspect)));

        // Only input `a` arrives for checkout [0,60): the bucket stays open, waiting on `b`.
        // With no `b` record, the watermark stays 0, so nothing is evicted.
        fa.send(wrec("checkout", 0, 60, 5.0)).await.unwrap();

        // The record crosses two channels (per-input forwarder -> merged), so poll until the
        // node's loop has folded it - race-free without guessing at timing.
        let snap = poll(&handle, |s| s.groups.len() == 1).await;
        assert_eq!(snap.kind, "join");
        let g = &snap.groups[0];
        assert_eq!((g.start_nanos, g.end_nanos), (0, 60));
        assert_eq!(g.labels["service.name"], "checkout");
        assert_eq!(g.value, None, "a bucket has no value until it fires");
        assert_eq!(g.inputs.get("a"), Some(&5.0));
        assert!(!g.inputs.contains_key("b"), "b has not arrived");
        assert_eq!(g.samples, 1, "one of two inputs filled");

        // `b` completes the bucket: it fires and leaves the open set. Reading the emitted
        // record proves the loop removed the bucket, so the next snapshot is deterministic.
        fb.send(wrec("checkout", 0, 60, 8.0)).await.unwrap();
        assert_eq!(
            out.recv().await.expect("the completed join fires").value,
            13.0
        );
        let snap = query(&handle).await;
        assert!(snap.groups.is_empty(), "a fired bucket leaves the snapshot");

        drop((fa, fb));
    }

    #[tokio::test]
    async fn max_groups_sheds_new_buckets() {
        // Feed only input `a`, so records arrive in one ordered stream (no cross-input race):
        // two keys fill the cap, the third opens no bucket and is metered as capped.
        let mut be = InProcess::new(64);
        let fa = be.producer("a", &KeySpec::Unkeyed);
        let fb = be.producer("b", &KeySpec::Unkeyed);
        let rxs = vec![be.consumer("a"), be.consumer("b")];
        let tx = be.producer("out", &KeySpec::Unkeyed);
        drop(be);
        let capped = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let metrics: SharedMetrics = Arc::new(CapCounter(capped.clone()));
        let nm = NodeMetrics::bind(&metrics, "j", NodeKind::Join);
        let spec = Spec {
            id: "j".into(),
            inputs: vec!["a".into(), "b".into()],
            name: None,
            value: Some("a + b".into()),
            max_groups: Some(2),
        };
        let node = tokio::spawn(run(spec, rxs, tx, nm, None));
        for svc in ["s1", "s2", "s3"] {
            fa.send(wrec(svc, 0, 60, 1.0)).await.unwrap();
        }
        drop((fa, fb)); // both inputs close, so the join drains and returns
        node.await.unwrap().unwrap();
        assert_eq!(
            capped.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "the third key was shed by max_groups"
        );
    }

    /// A `Metrics` that only counts drops with reason `Capped`, for the cap test.
    struct CapCounter(Arc<std::sync::atomic::AtomicU64>);
    impl crate::metrics::Metrics for CapCounter {
        fn node(&self, _: &str, _: NodeKind) -> Arc<dyn crate::metrics::NodeRecorder> {
            Arc::new(CapRec(self.0.clone()))
        }
    }
    struct CapRec(Arc<std::sync::atomic::AtomicU64>);
    impl crate::metrics::NodeRecorder for CapRec {
        fn record_out(&self) {}
        fn record_dropped(&self, n: u64, reason: DropReason) {
            if reason == DropReason::Capped {
                self.0.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            }
        }
        fn window_flushed(&self, _: u64) {}
        fn node_error(&self) {}
    }

    /// Ask the join node for its current snapshot.
    async fn query(handle: &tokio::sync::mpsc::Sender<crate::inspect::Query>) -> NodeSnapshot {
        let (reply, rx) = tokio::sync::oneshot::channel();
        handle.send(reply).await.expect("node is alive");
        rx.await.expect("node replied")
    }

    /// Poll the snapshot until `done`, tolerating the forwarder-to-merged channel hop.
    async fn poll(
        handle: &tokio::sync::mpsc::Sender<crate::inspect::Query>,
        done: impl Fn(&NodeSnapshot) -> bool,
    ) -> NodeSnapshot {
        for _ in 0..200 {
            let snap = query(handle).await;
            if done(&snap) {
                return snap;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        panic!("snapshot condition not met in time");
    }

    fn setup(
        input_ids: &[&str],
        value: Option<&str>,
    ) -> (Vec<Box<dyn Producer>>, Box<dyn Consumer>) {
        let mut be = InProcess::new(64);
        let feeders: Vec<_> = input_ids
            .iter()
            .map(|id| be.producer(id, &KeySpec::Unkeyed))
            .collect();
        let rxs: Vec<_> = input_ids.iter().map(|id| be.consumer(id)).collect();
        let tx = be.producer("out", &KeySpec::Unkeyed);
        let out = be.consumer("out");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "j", NodeKind::Join);
        let ids: Vec<String> = input_ids.iter().map(|s| s.to_string()).collect();
        let spec = Spec {
            id: "j".to_string(),
            inputs: ids,
            name: None,
            value: value.map(String::from),
            max_groups: None,
        };
        tokio::spawn(run(spec, rxs, tx, nm, None));
        (feeders, out)
    }

    fn wrec(svc: &str, start: u64, end: u64, v: f64) -> Record {
        let mut attrs = Attrs::new();
        attrs.insert("service.name".into(), AttrValue::Str(svc.into()));
        Record {
            ts_nanos: end,
            start_ts_nanos: Some(start),
            resource: Attrs::new(),
            scope: None,
            name: "src".into(),
            value: v,
            attrs,
        }
    }
}
