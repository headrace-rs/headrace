//! The window transform fires in *event time*: a window closes when a later record pushes
//! the watermark past its end, not on a wall clock. This exercises the async driver + the
//! in-process backend together, which the pure-`Window` unit tests don't cover. Event time
//! is driven entirely by the records, so no clock manipulation is needed.

use headrace_core::backend::{Backend, InProcess};
use headrace_core::metrics::{NodeKind, NodeMetrics};
use headrace_core::record::{Attrs, Record};
use headrace_core::{NoopMetrics, SharedMetrics};
use headrace_ir::Transform;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn window_fires_on_the_event_time_watermark() {
    let mut be = InProcess::new(64);
    let feed = be.producer("in"); // we push inputs here
    let win_rx = be.consumer("in"); // window reads them
    let win_tx = be.producer("w"); // window writes rollups
    let mut out = be.consumer("w"); // we read them
    drop(be);

    let m: SharedMetrics = Arc::new(NoopMetrics);
    let nm = NodeMetrics::bind(&m, "w", NodeKind::Window);

    // Transform is #[non_exhaustive]; build it through the parser, not a literal.
    let op: Transform =
        serde_yaml::from_str("type: window\nid: w\ninput: in\nsize: 5s\naggregate:\n  op: count")
            .unwrap();
    let task = tokio::spawn(headrace_core::transform::run(op, win_rx, win_tx, nm));

    let five_s = Duration::from_secs(5).as_nanos() as u64;
    // Three records land in the first window [0, 5s).
    for _ in 0..3 {
        feed.send(None, rec_at(1_000_000_000)).await.unwrap(); // ts = 1s
    }
    // A record at t = 5s advances the watermark to 5s, closing [0, 5s).
    feed.send(None, rec_at(five_s)).await.unwrap();

    let flushed = out
        .recv()
        .await
        .expect("window emits one aggregate when the watermark passes its end");
    assert_eq!(flushed.value, 3.0, "count of the three records in [0, 5s)");
    let start = flushed.start_ts_nanos.expect("window start set");
    assert_eq!(start, 0);
    assert_eq!(
        flushed.ts_nanos - start,
        five_s,
        "window [start, end) spans `size`"
    );

    task.abort();
}

/// With `idle_timeout` set, a window the watermark hasn't closed still fires once the
/// input goes quiet for that long. Virtual time makes the wait deterministic.
#[tokio::test(start_paused = true)]
async fn idle_timeout_collapses_an_open_window() {
    let mut be = InProcess::new(64);
    let feed = be.producer("in");
    let win_rx = be.consumer("in");
    let win_tx = be.producer("w");
    let mut out = be.consumer("w");
    drop(be);

    let m: SharedMetrics = Arc::new(NoopMetrics);
    let nm = NodeMetrics::bind(&m, "w", NodeKind::Window);

    // A 1h window never closes on the watermark here, but idle_timeout: 5s does.
    let op: Transform = serde_yaml::from_str(
        "type: window\nid: w\ninput: in\nsize: 1h\nidle_timeout: 5s\naggregate:\n  op: count",
    )
    .unwrap();
    let task = tokio::spawn(headrace_core::transform::run(op, win_rx, win_tx, nm));

    // Two records land in one open window (watermark stays far below its end).
    feed.send(None, rec_at(1_000_000_000)).await.unwrap();
    feed.send(None, rec_at(1_000_000_000)).await.unwrap();

    // Nothing else can progress, so virtual time advances to the 5s idle timer, which
    // collapses the open window. No manual sleep.
    let flushed = out
        .recv()
        .await
        .expect("idle timeout flushes the open window");
    assert_eq!(flushed.value, 2.0, "count of the two buffered records");

    task.abort();
}

fn rec_at(ts_nanos: u64) -> Record {
    Record {
        ts_nanos,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "m".into(),
        value: 1.0,
        attrs: Attrs::new(),
    }
}
