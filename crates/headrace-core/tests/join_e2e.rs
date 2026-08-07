//! End-to-end join: two `window` nodes feed a `join`, exercised through the real
//! transform dispatch and in-process backend. Records are event-time, so a later record
//! advances each window's watermark to fire the closed one; the join then aligns the two
//! rollups and reduces them. Also validates the shipped cross-series example.

use headrace_core::backend::{Backend, InProcess};
use headrace_core::metrics::{NodeKind, NodeMetrics};
use headrace_core::record::{AttrValue, Attrs, Record};
use headrace_core::{NoopMetrics, SharedMetrics};
use headrace_ir::{Pipeline, Transform};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn window_then_join_reduces_two_series() {
    let mut be = InProcess::new(64);
    let feed_hi = be.producer("hi_src");
    let feed_lo = be.producer("lo_src");
    let hi_rx = be.consumer("hi_src");
    let hi_tx = be.producer("hi");
    let lo_rx = be.consumer("lo_src");
    let lo_tx = be.producer("lo");
    let join_hi = be.consumer("hi");
    let join_lo = be.consumer("lo");
    let join_tx = be.producer("j");
    let mut out = be.consumer("j");
    drop(be);

    let m: SharedMetrics = Arc::new(NoopMetrics);
    let hi: Transform = window("hi", "hi_src");
    let lo: Transform = window("lo", "lo_src");
    let j: Transform =
        serde_yaml::from_str("type: join\nid: j\ninputs: [hi, lo]\nname: diff\nvalue: \"hi - lo\"")
            .unwrap();
    tokio::spawn(headrace_core::transform::run(
        hi,
        vec![hi_rx],
        hi_tx,
        NodeMetrics::bind(&m, "hi", NodeKind::Window),
    ));
    tokio::spawn(headrace_core::transform::run(
        lo,
        vec![lo_rx],
        lo_tx,
        NodeMetrics::bind(&m, "lo", NodeKind::Window),
    ));
    tokio::spawn(headrace_core::transform::run(
        j,
        vec![join_hi, join_lo],
        join_tx,
        NodeMetrics::bind(&m, "j", NodeKind::Join),
    ));

    let five_s = Duration::from_secs(5).as_nanos() as u64;
    // Each series gets one value in window [0, 5s); a record at 5s advances the watermark
    // and fires it.
    feed_hi
        .send(None, rec("checkout", 1_000_000_000, 10.0))
        .await
        .unwrap();
    feed_lo
        .send(None, rec("checkout", 1_000_000_000, 3.0))
        .await
        .unwrap();
    feed_hi
        .send(None, rec("checkout", five_s, 0.0))
        .await
        .unwrap();
    feed_lo
        .send(None, rec("checkout", five_s, 0.0))
        .await
        .unwrap();

    let got = out.recv().await.expect("the joined rollup for [0, 5s)");
    assert_eq!(got.name, "diff");
    assert_eq!(got.value, 7.0, "hi(10) - lo(3)");
    assert_eq!(got.start_ts_nanos, Some(0));
    assert_eq!(got.ts_nanos, five_s);
    assert_eq!(
        got.attrs.get("service.name"),
        Some(&AttrValue::Str("checkout".into()))
    );
}

#[test]
fn cross_series_example_is_valid() {
    let p: Pipeline = serde_yaml::from_str(include_str!("../../../examples/cross_series.yaml"))
        .expect("example parses");
    headrace_core::validate(&p).expect("example validates");
}

// A 5s max window keyed by service.name, reading `input`, emitting node `id`.
fn window(id: &str, input: &str) -> Transform {
    serde_yaml::from_str(&format!(
        "type: window\nid: {id}\ninput: {input}\nsize: 5s\ngroup_by: [service.name]\naggregate:\n  op: max\n  field: value"
    ))
    .unwrap()
}

fn rec(service: &str, ts_nanos: u64, value: f64) -> Record {
    let mut attrs = Attrs::new();
    attrs.insert("service.name".into(), AttrValue::Str(service.into()));
    Record {
        ts_nanos,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "src".into(),
        value,
        attrs,
    }
}
