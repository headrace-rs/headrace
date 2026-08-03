//! `filter`: forward records whose `key` exists (and equals `equals`, when set), drop the rest.
//! Stateless.

use crate::backend::{Consumer, Producer};
use crate::metrics::NodeMetrics;
use crate::record::Record;
use anyhow::Result;

/// Keep predicate, extracted so it can be tested without a channel.
fn keep(rec: &Record, key: &str, equals: &Option<String>) -> bool {
    match (rec.lookup(key), equals) {
        (Some(v), Some(want)) => v.to_string() == *want,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// Forward records that pass [`keep`], metering forwarded and dropped counts.
pub(super) async fn run(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{AttrValue, Attrs};

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
}

/// Exercises the async filter against mocked `Backend` handles - the boundary a
/// networked backend swaps into. Run with `--features mocks`.
#[cfg(all(test, feature = "mocks"))]
mod backend_tests {
    use super::*;
    use crate::backend::{MockConsumer, MockProducer};
    use crate::metrics::{Metrics, NodeKind, NodeRecorder, SharedMetrics};
    use crate::record::{AttrValue, Attrs};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

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

        run(
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
}
