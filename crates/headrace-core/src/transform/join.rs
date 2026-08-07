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
use crate::metrics::NodeMetrics;
use crate::record::{AttrValue, Attrs, Record};
use anyhow::{Result, anyhow};
use std::collections::HashMap;

struct Bucket {
    start: u64,
    end: u64,
    /// The shared `group_by` labels for this aligned key.
    labels: Attrs,
    /// One slot per input, filled as each input's value arrives.
    values: Vec<Option<f64>>,
    filled: usize,
}

pub(super) async fn run(
    id: String,
    inputs: Vec<String>,
    name: Option<String>,
    value: Option<String>,
    rxs: Vec<Box<dyn Consumer>>,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
) -> Result<()> {
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

    let mut buckets: HashMap<String, Bucket> = HashMap::new();
    let mut max_end = vec![0u64; n];

    while let Some((i, rec)) = merged_rx.recv().await {
        let start = rec.start_ts_nanos.unwrap_or(rec.ts_nanos);
        let end = rec.ts_nanos;
        max_end[i] = max_end[i].max(end);

        let key = bucket_key(start, end, &rec.attrs);
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
            match emit(&out_name, &inputs, expr.as_ref(), bucket)? {
                Some(record) => {
                    if tx.send(None, record).await.is_err() {
                        return Ok(());
                    }
                    nm.out();
                }
                None => nm.dropped(1), // the reduce expression could not be evaluated
            }
        }

        // Evict incomplete buckets every input has now advanced past.
        let watermark = max_end.iter().copied().min().unwrap_or(0);
        let stale: Vec<String> = buckets
            .iter()
            .filter(|(_, b)| b.end <= watermark)
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale {
            buckets.remove(&k);
            nm.dropped(1);
        }
    }
    Ok(())
}

/// Build the output record for a complete bucket. With an expression, evaluate it against
/// the per-input values (exposed as attributes by id) and emit a clean record (labels
/// only); without one, carry the per-input values as attributes. `None` if the expression
/// could not be evaluated.
fn emit(
    name: &str,
    inputs: &[String],
    expr: Option<&Expr>,
    bucket: Bucket,
) -> Result<Option<Record>> {
    let value = match expr {
        Some(expr) => {
            let mut probe = bucket.labels.clone();
            carry_inputs(&mut probe, inputs, &bucket.values);
            let record = record(name, bucket.start, bucket.end, 0.0, probe);
            match expr.eval(&record) {
                Ok(v) if v.is_finite() => v,
                _ => return Ok(None),
            }
        }
        None => 0.0,
    };
    let mut attrs = bucket.labels;
    if expr.is_none() {
        carry_inputs(&mut attrs, inputs, &bucket.values);
    }
    Ok(Some(record(name, bucket.start, bucket.end, value, attrs)))
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

/// A stable key for an aligned bucket: window bounds plus the sorted labels.
fn bucket_key(start: u64, end: u64, labels: &Attrs) -> String {
    use std::fmt::Write;
    let mut k = format!("{start}\u{1f}{end}");
    for (name, v) in labels {
        let _ = write!(k, "\u{1f}{name}={v}");
    }
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Backend, InProcess};
    use crate::metrics::{NodeKind, NodeMetrics};
    use crate::{NoopMetrics, SharedMetrics};
    use std::sync::Arc;

    #[tokio::test]
    async fn reduces_aligned_inputs() {
        let (feeders, mut out) = setup(&["a", "b"], Some("a - b"));
        feeders[0]
            .send(None, wrec("checkout", 0, 60, 214.0))
            .await
            .unwrap();
        feeders[1]
            .send(None, wrec("checkout", 0, 60, 190.0))
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
        feeders[0]
            .send(None, wrec("checkout", 0, 60, 5.0))
            .await
            .unwrap();
        feeders[1]
            .send(None, wrec("checkout", 0, 60, 8.0))
            .await
            .unwrap();
        let got = out.recv().await.expect("joined record");
        assert_eq!(got.attrs.get("a"), Some(&AttrValue::Double(5.0)));
        assert_eq!(got.attrs.get("b"), Some(&AttrValue::Double(8.0)));
        drop(feeders);
    }

    #[tokio::test]
    async fn drops_unaligned_windows() {
        let (feeders, mut out) = setup(&["a", "b"], Some("a + b"));
        // `a` has window [0,60); `b` jumps to [60,120), so [0,60) never completes.
        feeders[0]
            .send(None, wrec("checkout", 0, 60, 1.0))
            .await
            .unwrap();
        feeders[1]
            .send(None, wrec("checkout", 60, 120, 2.0))
            .await
            .unwrap();
        // `a` advances to [60,120) too, completing it; [0,60) is evicted, never emitted.
        feeders[0]
            .send(None, wrec("checkout", 60, 120, 3.0))
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
        feeders[0]
            .send(None, wrec("checkout", 0, 60, 5.0))
            .await
            .unwrap();
        feeders[1]
            .send(None, wrec("checkout", 0, 60, 8.0))
            .await
            .unwrap();
        drop(feeders); // the [0,60) bucket completes, the reduce fails, nothing forwards
        assert!(out.recv().await.is_none());
    }

    fn setup(
        input_ids: &[&str],
        value: Option<&str>,
    ) -> (Vec<Box<dyn Producer>>, Box<dyn Consumer>) {
        let mut be = InProcess::new(64);
        let feeders: Vec<_> = input_ids.iter().map(|id| be.producer(id)).collect();
        let rxs: Vec<_> = input_ids.iter().map(|id| be.consumer(id)).collect();
        let tx = be.producer("out");
        let out = be.consumer("out");
        drop(be);
        let metrics: SharedMetrics = Arc::new(NoopMetrics);
        let nm = NodeMetrics::bind(&metrics, "j", NodeKind::Join);
        let ids: Vec<String> = input_ids.iter().map(|s| s.to_string()).collect();
        tokio::spawn(run(
            "j".to_string(),
            ids,
            None,
            value.map(String::from),
            rxs,
            tx,
            nm,
        ));
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
