//! `window` answers live state-inspection queries (ADR-0014) from its own task loop, so a
//! snapshot reflects the folds already applied and never races the aggregation. This
//! exercises the async driver's inspect arm, which the pure-`Window` unit tests don't.

use headrace_core::backend::{Backend, InProcess};
use headrace_core::inspect::{NodeSnapshot, Query};
use headrace_core::metrics::{NodeKind, NodeMetrics};
use headrace_core::record::{Attrs, Record};
use headrace_core::{NoopMetrics, SharedMetrics};
use headrace_ir::Transform;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

const SEC: u64 = 1_000_000_000;

#[tokio::test]
async fn window_answers_a_query_from_its_own_loop() {
    let mut be = InProcess::new(64);
    let feed = be.producer("in");
    let win_rx = be.consumer("in");
    let win_tx = be.producer("w");
    let mut out = be.consumer("w");
    drop(be);

    let m: SharedMetrics = Arc::new(NoopMetrics);
    let nm = NodeMetrics::bind(&m, "w", NodeKind::Window);
    let (handle, inspector) = mpsc::channel::<Query>(4);

    // #[non_exhaustive]: build the transform through the parser, not a literal.
    let op: Transform =
        serde_yaml::from_str("type: window\nid: w\ninput: in\nsize: 5s\naggregate:\n  op: count")
            .unwrap();
    let task = tokio::spawn(headrace_core::transform::run(
        op,
        vec![win_rx],
        win_tx,
        nm,
        Some(inspector),
    ));

    // Three records in [0, 5s), then one at 6s that advances the watermark and fires it.
    for _ in 0..3 {
        feed.send(None, rec_at(SEC)).await.unwrap();
    }
    feed.send(None, rec_at(6 * SEC)).await.unwrap();

    // Reading the flush proves the loop has processed past the 6s record - so [5s, 10s) is
    // now the one open window, holding that single record. This makes the query race-free:
    // the input channel is drained, so the snapshot is deterministic.
    let flushed = out.recv().await.expect("[0, 5s) fires on the watermark");
    assert_eq!(flushed.value, 3.0);

    let snap = snapshot(&handle).await;
    assert_eq!(snap.kind, "window");
    assert_eq!(snap.groups.len(), 1, "only [5s, 10s) is still open");
    let g = &snap.groups[0];
    assert_eq!((g.start_nanos, g.end_nanos), (5 * SEC, 10 * SEC));
    assert_eq!(g.value, Some(1.0), "one record folded so far");
    assert_eq!(g.samples, 1);
    assert!(g.labels.is_empty(), "unkeyed window");

    // Every Handle dropped: the node stops polling for queries but keeps running - a later
    // watermark advance still fires the open window.
    drop(handle);
    feed.send(None, rec_at(11 * SEC)).await.unwrap();
    let flushed = out
        .recv()
        .await
        .expect("window still flushes after inspection ends");
    assert_eq!(flushed.value, 1.0);

    drop(feed);
    task.await.unwrap().unwrap();
}

/// Ask the node for its current snapshot, produced by the node's own loop.
async fn snapshot(handle: &mpsc::Sender<Query>) -> NodeSnapshot {
    let (reply, rx) = oneshot::channel();
    handle.send(reply).await.expect("node is alive");
    rx.await.expect("node replied with a snapshot")
}

fn rec_at(ts: u64) -> Record {
    Record {
        ts_nanos: ts,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "m".into(),
        value: 1.0,
        attrs: Attrs::new(),
    }
}
