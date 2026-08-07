//! OTLP end-to-end over real gRPC on loopback, no external services: a client feeds the
//! OTLP source, records fold through a window rollup, and the OTLP sink exports the result
//! to an in-test mock collector. This covers what the `convert` and pure-`Window` unit
//! tests cannot - the source/sink dispatch, the tonic server and client, and the drain
//! path that flushes when the upstream closes.
#![cfg(feature = "otlp")]

use headrace_core::backend::{Backend, InProcess};
use headrace_core::metrics::{NodeKind, NodeMetrics};
use headrace_core::otlp::convert::{decode, encode};
use headrace_core::otlp::normalize::Normalizer;
use headrace_core::record::{AttrValue, Attrs, Record};
use headrace_core::{NoopMetrics, SharedMetrics};
use headrace_ir::{Sink, Source, Transform};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_client::MetricsServiceClient,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::transport::{Channel, Server};
use tonic::{Request, Response, Status};

/// A stand-in downstream collector: it records every export request it receives.
#[derive(Clone)]
struct MockCollector {
    seen: mpsc::UnboundedSender<ExportMetricsServiceRequest>,
}

#[tonic::async_trait]
impl MetricsService for MockCollector {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let _ = self.seen.send(request.into_inner());
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// A loopback address with a free port. The bind/drop/reuse gap is a small race that is
/// acceptable for a local test.
fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = l.local_addr().expect("read local addr");
    drop(l);
    format!("127.0.0.1:{}", addr.port())
}

/// Connect to an OTLP endpoint, retrying until the server is listening.
async fn connect_ready(endpoint: &str) -> MetricsServiceClient<Channel> {
    for _ in 0..100 {
        if let Ok(client) = MetricsServiceClient::connect(endpoint.to_string()).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("OTLP server at {endpoint} never became ready");
}

/// A realistic event time (nanos since the epoch, ~2023). Kept away from 0 so the
/// window's start survives the OTLP round trip, where `start_time_unix_nano == 0` means
/// "unset". All points share it, so they land in one event-time window.
const TS_NANOS: u64 = 1_700_000_000_000_000_000;

/// One gauge point for `service` with the given value.
fn point(service: &str, value: f64) -> Record {
    let mut attrs = Attrs::new();
    attrs.insert("service.name".into(), AttrValue::Str(service.into()));
    Record {
        ts_nanos: TS_NANOS,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "http.server.duration".into(),
        value,
        attrs,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn otlp_round_trips_through_a_window_rollup() {
    // 1. A mock collector for the sink to export to.
    let (seen_tx, mut seen_rx) = mpsc::unbounded_channel();
    let collector_addr = free_addr();
    let collector_endpoint = format!("http://{collector_addr}");
    let collector_task = tokio::spawn(
        Server::builder()
            .add_service(MetricsServiceServer::new(MockCollector { seen: seen_tx }))
            .serve(collector_addr.parse().expect("parse collector addr")),
    );
    connect_ready(&collector_endpoint).await; // up before the sink connects

    // 2. Wire receiver -> window -> exporter over an in-process backend.
    let mut be = InProcess::new(1024);
    let recv_out = be.producer("in");
    let win_in = be.consumer("in");
    let win_out = be.producer("w");
    let sink_in = be.consumer("w");
    drop(be); // release retained senders so channel-close propagates on drain

    let metrics: SharedMetrics = Arc::new(NoopMetrics);
    let listen = free_addr();
    let source: Source =
        serde_yaml::from_str(&format!("type: otlp\nid: in\nlisten: {listen}")).unwrap();
    // A 1h window never ticks during the test: every point folds into the single flush on
    // drain, so the collector sees exactly one rollup.
    let window: Transform = serde_yaml::from_str(
        "type: window\nid: w\ninput: in\nsize: 1h\ngroup_by: [service.name]\naggregate:\n  op: avg\n  field: value",
    )
    .unwrap();
    let sink: Sink = serde_yaml::from_str(&format!(
        "type: otlp\nid: out\ninput: w\nendpoint: {collector_endpoint}"
    ))
    .unwrap();

    let recv_task = tokio::spawn(headrace_core::source::run(
        source,
        recv_out,
        NodeMetrics::bind(&metrics, "in", NodeKind::Source),
    ));
    let win_task = tokio::spawn(headrace_core::transform::run(
        window,
        vec![win_in],
        win_out,
        NodeMetrics::bind(&metrics, "w", NodeKind::Window),
    ));
    let sink_task = tokio::spawn(headrace_core::sink::run(
        sink,
        sink_in,
        NodeMetrics::bind(&metrics, "out", NodeKind::Sink),
    ));

    // 3. Push three points for one service over real gRPC.
    let mut client = connect_ready(&format!("http://{listen}")).await;
    let req = encode(vec![
        point("checkout", 10.0),
        point("checkout", 20.0),
        point("checkout", 30.0),
    ]);
    client.export(req).await.expect("export to the receiver");

    // 4. Close ingress. Dropping the client ends the connection and aborting the receiver
    //    drops its held producer, so "in" closes: the window flushes on drain, which closes
    //    "w", so the exporter flushes and exports to the collector.
    drop(client);
    recv_task.abort();

    // 5. The collector must receive exactly the rollup: avg(10, 20, 30) = 20 for checkout.
    let got = tokio::time::timeout(Duration::from_secs(5), seen_rx.recv())
        .await
        .expect("collector received an export within 5s")
        .expect("collector channel stayed open");
    let rollup = decode(got, &mut Normalizer::default());
    assert_eq!(rollup.len(), 1, "one group yields one rollup record");
    assert_eq!(rollup[0].name, "http.server.duration");
    assert_eq!(rollup[0].value, 20.0, "avg of 10, 20, 30");
    assert_eq!(
        rollup[0].attrs.get("service.name"),
        Some(&AttrValue::Str("checkout".into())),
    );
    assert!(
        rollup[0].start_ts_nanos.is_some(),
        "rollup carries window bounds"
    );

    win_task.abort();
    sink_task.abort();
    collector_task.abort();
}
