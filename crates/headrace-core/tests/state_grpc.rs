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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_streams_window_state_as_it_changes() {
    use headrace_proto::v1::WatchRequest;

    let addr = free_addr();
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

    // Connect once the server is up, then open a Watch stream.
    let endpoint = format!("http://{addr}");
    let mut client = None;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(c) = StateClient::connect(endpoint.clone()).await {
            client = Some(c);
            break;
        }
    }
    let mut stream = client
        .expect("server came up")
        .watch(WatchRequest { node: vec![] })
        .await
        .expect("watch opens")
        .into_inner();

    // The generator keeps folding records, so the window publishes a change event per record.
    let event = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("a watch event arrives within 5s")
        .expect("stream is not errored")
        .expect("stream yields a node state");
    assert_eq!(event.id, "w");
    assert_eq!(event.kind, "window");
    assert!(
        event
            .groups
            .iter()
            .any(|g| g.labels.contains_key("service.name")),
        "the streamed snapshot carries labelled groups"
    );

    run.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_shutdown_stops_serving() {
    use headrace_core::inspect::{Registry, server};

    let addr = free_addr();
    let server = server::spawn(Registry::default(), addr);
    let endpoint = format!("http://{addr}");

    // Wait until it answers a Get (an empty registry replies with no nodes).
    let mut up = false;
    for _ in 0..200 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if let Ok(mut c) = StateClient::connect(endpoint.clone()).await
            && c.get(GetRequest { node: vec![] }).await.is_ok()
        {
            up = true;
            break;
        }
    }
    assert!(up, "server never came up");

    server.shutdown().await;

    // After a graceful shutdown the port is closed, so a fresh connect fails.
    assert!(
        StateClient::connect(endpoint).await.is_err(),
        "server should stop accepting once shut down"
    );
}
