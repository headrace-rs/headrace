#![cfg(feature = "nats")]
//! End-to-end test of the NATS JetStream backend against a real server (via testcontainers).
//!
//! `#[ignore]` by default because it needs Docker; the `Integration` CI job runs it with
//! `-- --ignored`. It exercises the network path the unit tests cannot: connect, stream
//! provisioning, publish with ack, durable pull consume, and MessagePack round-trip.

use headrace_core::backend::{Backend, KeySpec, Nats};
use headrace_core::record::{AttrValue, Attrs, Record};
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

#[tokio::test]
#[ignore = "needs Docker; run in the Integration CI job with -- --ignored"]
async fn records_round_trip_through_jetstream() {
    // `-js` enables JetStream. NATS logs readiness to stderr; the connect retry below also
    // absorbs any startup race.
    let container = GenericImage::new("nats", "2.10")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start the nats container");
    let port = container
        .get_host_port_ipv4(4222.tcp())
        .await
        .expect("mapped 4222");
    let url = format!("nats://127.0.0.1:{port}");

    // Retry connect so a slightly-early readiness signal does not flake the test.
    let mut backend = None;
    for _ in 0..30 {
        match Nats::connect(&url, "test", &["w".to_string()]).await {
            Ok(b) => {
                backend = Some(b);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    let mut backend = backend.expect("connect to jetstream");

    let producer = backend.producer("w", &KeySpec::Unkeyed);
    let mut consumer = backend.consumer("w");

    for i in 0..3u64 {
        producer.send(rec(i)).await.expect("publish");
    }
    // Work-queue + single consumer preserves order.
    for i in 0..3u64 {
        let got = consumer.recv().await.expect("a record arrives");
        assert_eq!(got.value, i as f64, "records arrive in publish order");
        assert_eq!(got.name, "m");
        assert_eq!(
            got.attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into())),
            "attributes survive the MessagePack round trip"
        );
    }
}

#[tokio::test]
#[ignore = "needs Docker; run in the Integration CI job with -- --ignored"]
async fn connect_waits_for_nats_then_recovers() {
    // Pin a host port so we know the URL before NATS exists.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let url = format!("nats://127.0.0.1:{port}");

    // Start connecting while NATS is down; it must keep retrying, not error out.
    let connecting = tokio::spawn({
        let url = url.clone();
        async move { Nats::connect(&url, "test", &["w".to_string()]).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !connecting.is_finished(),
        "connect should wait for NATS, not fail while it is down"
    );

    // Bring NATS up on the pinned port; connect then completes and the backend works.
    let _container = GenericImage::new("nats", "2.10")
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_mapped_port(port, 4222.tcp())
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start the nats container");

    let mut backend = tokio::time::timeout(Duration::from_secs(20), connecting)
        .await
        .expect("connect finishes within 20s of NATS coming up")
        .expect("connect task ok")
        .expect("connected once NATS is up");
    let producer = backend.producer("w", &KeySpec::Unkeyed);
    let mut consumer = backend.consumer("w");
    producer.send(rec(7)).await.expect("publish after recovery");
    assert_eq!(consumer.recv().await.expect("a record arrives").value, 7.0);
}

fn rec(i: u64) -> Record {
    let mut attrs = Attrs::new();
    attrs.insert("service.name".into(), AttrValue::Str("checkout".into()));
    Record {
        ts_nanos: i,
        start_ts_nanos: None,
        resource: Attrs::new(),
        scope: None,
        name: "m".into(),
        value: i as f64,
        attrs,
    }
}
