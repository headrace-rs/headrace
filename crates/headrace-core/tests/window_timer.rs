//! The window transform flushes on a timer, driven by *virtual* time so the test is
//! deterministic - no wall-clock sleeps. Exercises the async driver + in-process backend
//! together, which the pure-`Window` unit tests don't cover.

use headrace_core::backend::{Backend, InProcess};
use headrace_core::metrics::{NodeKind, NodeMetrics};
use headrace_core::record::{Attrs, Record};
use headrace_core::{NoopMetrics, SharedMetrics};
use headrace_ir::Transform;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn window_flushes_on_the_timer() {
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

    for _ in 0..3 {
        feed.send(None, rec(1.0)).await.unwrap();
    }
    // With the clock paused, the runtime auto-advances to the next timer once nothing else can
    // progress: the three records fold first (recv is ready, the tick is not), then time jumps
    // to the 5s boundary and the window flushes. No sleeps, no manual advance.
    let flushed = out
        .recv()
        .await
        .expect("window emits one aggregate on the timer");
    assert_eq!(flushed.value, 3.0, "count of the three folded records");
    let start = flushed.start_ts_nanos.expect("window start set");
    assert_eq!(
        flushed.ts_nanos - start,
        Duration::from_secs(5).as_nanos() as u64,
        "window [start,end) spans `size`"
    );

    task.abort();
}

fn rec(v: f64) -> Record {
    Record {
        ts_nanos: 1,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "m".into(),
        value: v,
        attrs: Attrs::new(),
    }
}
