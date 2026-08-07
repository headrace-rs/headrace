#![cfg(feature = "inspect")]
//! End to end: `headrace run --inspect-addr` serves live window state over the `State` gRPC
//! API (ADR-0014). Drives the real runtime with a generator, then queries it with the
//! generated client - exercising the registry wiring, the server, and the proto mapping.

use headrace_core::backend::InProcess;
use headrace_core::{NoopMetrics, RunOptions, SharedMetrics};
use headrace_ir::Pipeline;
use headrace_proto::v1::GetRequest;
use headrace_proto::v1::state_client::StateClient;
use std::sync::Arc;
use std::time::Duration;

/// A loopback address with a free port. The bind/drop/reuse gap is a small race, acceptable
/// in a test.
fn free_addr() -> std::net::SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = l.local_addr().expect("read local addr");
    drop(l);
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn state_get_reports_live_window_state() {
    let addr = free_addr();
    // A 1h window never fires during the test, so records accumulate as open state. The
    // generator cycles `service.name`, giving labelled groups to observe.
    let p: Pipeline = serde_yaml::from_str(
        r#"
        sources: [{ type: generator, id: gen, interval: 1ms }]
        transforms:
          - { type: window, id: w, input: gen, size: 1h,
              group_by: [service.name], aggregate: { op: count } }
        sinks: [{ type: stdout, id: out, input: w, format: json }]
        "#,
    )
    .expect("valid pipeline");
    let metrics: SharedMetrics = Arc::new(NoopMetrics);
    let opts = RunOptions {
        inspect_addr: Some(addr),
    };
    let run = tokio::spawn(headrace_core::run(p, InProcess::default(), metrics, opts));

    // Poll until the server is up and the window has folded at least one group.
    let endpoint = format!("http://{addr}");
    let mut nodes = Vec::new();
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let Ok(mut client) = StateClient::connect(endpoint.clone()).await else {
            continue;
        };
        let Ok(resp) = client.get(GetRequest { node: vec![] }).await else {
            continue;
        };
        let got = resp.into_inner().nodes;
        if got.iter().any(|n| !n.groups.is_empty()) {
            nodes = got;
            break;
        }
    }

    let w = nodes
        .iter()
        .find(|n| n.id == "w")
        .expect("State.Get reports the window node");
    assert_eq!(w.kind, "window");
    let g = w.groups.first().expect("at least one open group");
    assert!(
        g.labels.contains_key("service.name"),
        "group carries its group_by label"
    );
    assert!(g.samples >= 1, "records have been folded");
    assert_eq!(
        g.value,
        Some(g.samples as f64),
        "count equals the folded record count"
    );
    assert!(g.inputs.is_empty(), "inputs are a join concern");

    run.abort();
}
