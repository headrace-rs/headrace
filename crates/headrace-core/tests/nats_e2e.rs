#![cfg(feature = "nats")]
//! End-to-end test of the NATS JetStream backend against a real server (via testcontainers).
//!
//! `#[ignore]` by default because it needs Docker; the `Integration` CI job runs it with
//! `-- --ignored`. It exercises the network path the unit tests cannot: connect, stream
//! provisioning, publish with ack, durable pull consume, MessagePack round-trip, and the
//! static partitioning that splits an edge across workers.

use headrace_core::backend::{Backend, Consumer, KeySpec, Nats, PartitionConfig};
use headrace_core::record::{AttrValue, Attrs, Record};
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

#[tokio::test]
#[ignore = "needs Docker; run in the Integration CI job with -- --ignored"]
async fn records_round_trip_through_jetstream() {
    let (_container, url) = start_nats().await;

    // Four partitions, one worker (so it owns them all): exercises the multi-partition bind
    // and merge. Keying every record by the same value sends them to one partition, so
    // work-queue + single consumer preserves order.
    let mut backend = connect(&url, "test", part(4, 1, 0)).await;
    let producer = backend.producer("w", &KeySpec::Keyed(vec!["service.name".into()]));
    let mut consumer = backend.consumer("w");

    for i in 0..3u64 {
        producer.send(rec("checkout", i)).await.expect("publish");
    }
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
        async move { Nats::connect(&url, "test", &["w".to_string()], part(1, 1, 0)).await }
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
    producer
        .send(rec("checkout", 7))
        .await
        .expect("publish after recovery");
    assert_eq!(consumer.recv().await.expect("a record arrives").value, 7.0);
}

#[tokio::test]
#[ignore = "needs Docker; run in the Integration CI job with -- --ignored"]
async fn two_workers_split_partitions_by_key() {
    let (_container, url) = start_nats().await;

    // Six partitions, two workers: worker 0 owns {0,2,4}, worker 1 owns {1,3,5}. Both share
    // the same provisioned streams.
    let mut be0 = connect(&url, "split", part(6, 2, 0)).await;
    let mut be1 = connect(&url, "split", part(6, 2, 1)).await;

    // One keyed producer routes every record; each of 24 keys is sent twice.
    let producer = be0.producer("w", &KeySpec::Keyed(vec!["service.name".into()]));
    let keys: Vec<String> = (0..24).map(|k| format!("svc-{k}")).collect();
    for (v, key) in keys.iter().enumerate() {
        for _ in 0..2 {
            producer.send(rec(key, v as u64)).await.expect("publish");
        }
    }

    let mut c0 = be0.consumer("w");
    let mut c1 = be1.consumer("w");
    let got0 = drain(&mut c0).await;
    let got1 = drain(&mut c1).await;

    // Every record is delivered exactly once, to exactly one worker.
    assert_eq!(got0.len() + got1.len(), 48, "no record lost or duplicated");
    let (by0, by1) = (counts(&got0), counts(&got1));
    for key in &keys {
        let n0 = by0.get(key).copied().unwrap_or(0);
        let n1 = by1.get(key).copied().unwrap_or(0);
        assert_eq!(n0 + n1, 2, "{key} delivered twice in total");
        // Co-location: both copies of a key land on the same worker, never split.
        assert!(n0 == 0 || n1 == 0, "{key} must not straddle both workers");
    }
    // The split actually distributed work across both workers.
    assert!(
        !got0.is_empty() && !got1.is_empty(),
        "both workers get a share"
    );
}

#[tokio::test]
#[ignore = "needs Docker; run in the Integration CI job with -- --ignored"]
async fn a_duplicate_worker_index_is_rejected() {
    let (_container, url) = start_nats().await;

    let be0 = connect(&url, "lease", part(4, 2, 0)).await;
    let _held = be0
        .claim_worker_lease()
        .await
        .expect("the first worker claims index 0");

    // A second worker with the same index must fail fast, not silently split state.
    let dup = connect(&url, "lease", part(4, 2, 0)).await;
    let err = dup
        .claim_worker_lease()
        .await
        .expect_err("a duplicate index is rejected");
    assert!(err.to_string().contains("already held"), "{err}");

    // A different index is free to claim.
    let be1 = connect(&url, "lease", part(4, 2, 1)).await;
    be1.claim_worker_lease()
        .await
        .expect("a different index claims");
}

/// Retry connect so a slightly-early readiness signal does not flake the test.
async fn connect(url: &str, pipeline: &str, part: PartitionConfig) -> Nats {
    for _ in 0..30 {
        if let Ok(b) = Nats::connect(url, pipeline, &["w".to_string()], part).await {
            return b;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("connect to jetstream at {url}");
}

/// Read records until the stream goes quiet for 2s (this worker has no more).
async fn drain(consumer: &mut Box<dyn Consumer>) -> Vec<Record> {
    let mut got = Vec::new();
    while let Ok(Some(rec)) = tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
        got.push(rec);
    }
    got
}

/// Count records per `service.name`.
fn counts(recs: &[Record]) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for r in recs {
        let svc = r
            .attrs
            .get("service.name")
            .expect("keyed record")
            .to_string();
        *m.entry(svc).or_insert(0) += 1;
    }
    m
}

/// Start a JetStream-enabled NATS container and return it with its client URL. `-js` enables
/// JetStream; NATS logs readiness to stderr, and `connect`'s retry absorbs any startup race.
async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
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
    (container, format!("nats://127.0.0.1:{port}"))
}

fn part(partitions: u32, workers: u32, index: u32) -> PartitionConfig {
    PartitionConfig {
        partitions,
        workers,
        index,
    }
}

fn rec(svc: &str, i: u64) -> Record {
    let mut attrs = Attrs::new();
    attrs.insert("service.name".into(), AttrValue::Str(svc.into()));
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
