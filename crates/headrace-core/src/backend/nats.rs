//! NATS JetStream backend (ADR-0003, ADR-0015): a durable, back-pressured edge between
//! nodes for the scaled deployment. Records cross the wire as MessagePack, delivery is
//! at-least-once via ack-after-processing, and each edge is a work-queue stream split into
//! `P` partitions. A record is routed by `hash(key) % P` (fixed key-groups, the Flink
//! model, ADR-0008), computed client-side so provisioning stays self-contained and the
//! partition math is a pure function. Worker `i` of `N` owns the partitions where
//! `p % N == i`, so every record for a key lands on one worker and its keyed state never
//! moves. A single worker (`N = 1`) owns all partitions, which is the single-worker case.
//!
//! The deterministic parts - subject/stream naming, the codec, the partition math, the
//! stream and consumer config - are pure functions, unit-tested below. Only the thin async
//! glue (connect, publish, pull) needs a live server, covered by the `nats_e2e` test.

use crate::backend::{Backend, Consumer, KeySpec, Producer};
use crate::record::{AttrValue, Record};
use anyhow::{Context, Result, bail};
use async_nats::ConnectOptions;
use async_nats::jetstream::{self, consumer::pull, kv, stream};
use async_trait::async_trait;
use bytes::Bytes;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio_stream::{StreamExt, StreamMap};

/// How an edge's `P` partitions are split across workers. `partitions` is fixed for the
/// life of a stream; `index` of `workers` identifies this worker (a StatefulSet ordinal in
/// Kubernetes). A single worker (`workers == 1`) owns every partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionConfig {
    pub partitions: u32,
    pub workers: u32,
    pub index: u32,
}

impl PartitionConfig {
    /// Reject shapes that cannot describe a valid assignment. `workers <= partitions` keeps
    /// every worker busy (a worker with no partitions would idle); changing `workers` needs
    /// a restart and does not migrate state (ADR-0008).
    pub fn validate(&self) -> Result<()> {
        if self.partitions == 0 {
            bail!("partitions must be >= 1");
        }
        if self.workers == 0 {
            bail!("workers must be >= 1");
        }
        if self.index >= self.workers {
            bail!(
                "worker-index {} must be < workers {}",
                self.index,
                self.workers
            );
        }
        if self.workers > self.partitions {
            bail!(
                "workers {} must be <= partitions {}",
                self.workers,
                self.partitions
            );
        }
        Ok(())
    }

    /// The partitions this worker owns: `{ p in 0..P : p % N == index }`. Across all workers
    /// these are disjoint and cover `0..P`.
    fn owned(&self) -> Vec<u32> {
        (0..self.partitions)
            .filter(|p| p % self.workers == self.index)
            .collect()
    }
}

/// The subject partition `p` of a node's output records is published to. `<pipeline>`
/// namespaces subjects so many pipelines can share one cluster.
fn subject(pipeline: &str, node: &str, p: u32) -> String {
    format!("hr.{pipeline}.{node}.{p}")
}

/// The subject wildcard covering every partition of a node's output, for the stream.
fn wildcard_subject(pipeline: &str, node: &str) -> String {
    format!("hr.{pipeline}.{node}.*")
}

/// The JetStream stream name for a node output. Stream names cannot contain `.`, so the
/// subject's dots become underscores.
fn stream_name(pipeline: &str, node: &str) -> String {
    format!("hr_{pipeline}_{node}")
}

/// The durable pull-consumer name for one partition of a node output (its single reader).
fn durable_name(pipeline: &str, node: &str, p: u32) -> String {
    format!("{}_{p}_sink", stream_name(pipeline, node))
}

/// The work-queue stream config for a node output: it captures every partition subject, and
/// a record leaves the stream once its single (per-partition) consumer acks it.
fn stream_config(pipeline: &str, node: &str) -> stream::Config {
    stream::Config {
        name: stream_name(pipeline, node),
        subjects: vec![wildcard_subject(pipeline, node)],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    }
}

/// The durable pull-consumer config for one partition. The `filter_subject` binds it to a
/// single partition; work-queue retention requires consumers to have non-overlapping
/// filters, which per-partition subjects satisfy. Explicit acks drive ack-after-processing.
fn consumer_config(durable: &str, filter_subject: String) -> pull::Config {
    pull::Config {
        durable_name: Some(durable.to_string()),
        filter_subject,
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ..Default::default()
    }
}

fn encode(rec: &Record) -> Result<Bytes> {
    Ok(Bytes::from(
        rmp_serde::to_vec(rec).context("encoding record")?,
    ))
}

fn decode(payload: &[u8]) -> Result<Record> {
    rmp_serde::from_slice(payload).context("decoding record")
}

/// Canonical bytes for a record's key under `group_by`, in field order. Each field is a type
/// tag plus its value (length-prefixed for strings, so `["a","bc"]` and `["ab","c"]` differ),
/// and an absent field is a distinct tag. Types stay distinct (`Int(1)` != `Str("1")`); this
/// need not equal the window's in-memory group key, only be identical on every edge so a key
/// routes to the same partition throughout the graph.
fn key_bytes(rec: &Record, group_by: &[String]) -> Vec<u8> {
    let mut buf = Vec::new();
    for field in group_by {
        match rec.lookup(field) {
            None => buf.push(0),
            Some(AttrValue::Bool(b)) => {
                buf.push(1);
                buf.push(*b as u8);
            }
            Some(AttrValue::Int(i)) => {
                buf.push(2);
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Some(AttrValue::Double(d)) => {
                buf.push(3);
                buf.extend_from_slice(&d.to_bits().to_le_bytes());
            }
            Some(AttrValue::Str(s)) => {
                buf.push(4);
                buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
        }
    }
    buf
}

/// The partition a key routes to: FNV-1a-64 over the key, modulo `partitions`. Hand-rolled
/// so it is stable across versions and platforms (std's `DefaultHasher` is neither), which a
/// networked, rolling-upgraded backend needs. `partitions` is `>= 1` (checked at connect).
fn partition(key: &[u8], partitions: u32) -> u32 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    (h % partitions as u64) as u32
}

/// The partition a producer sends `rec` to: a keyed edge hashes the record's key so its
/// state co-locates on one worker; an unkeyed edge (feeding a stateless node) round-robins
/// `rr` for an even spread.
fn route(key: &KeySpec, rec: &Record, partitions: u32, rr: &AtomicU32) -> u32 {
    match key {
        KeySpec::Keyed(fields) => partition(&key_bytes(rec, fields), partitions),
        KeySpec::Unkeyed => rr.fetch_add(1, Ordering::Relaxed) % partitions,
    }
}

/// Exponential backoff for a retry loop, capped at 5s: 100ms, 200ms, 400ms, ... 5s, 5s.
fn backoff(attempt: u32) -> Duration {
    let ms = 100u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(ms.min(5_000))
}

/// How long a worker's ownership lease survives without a renewal (ADR-0016). The holder re-puts
/// its key every third of this, so a crashed worker's index frees after at most this long.
const LEASE_TTL: Duration = Duration::from_secs(30);

/// The KV bucket holding one ownership lease per worker index for a pipeline.
fn workers_bucket(pipeline: &str) -> String {
    format!("hr_{pipeline}_workers")
}

/// A human-readable identity for a lease value. Informational only - the create-only claim, not
/// the value, enforces exclusivity. Prefers the pod/host name, else the pid.
fn worker_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| format!("pid-{}", std::process::id()))
}

/// A NATS JetStream backend. Cheap to make handles from; the network connection is made
/// once in [`Nats::connect`], which also provisions a stream per output.
pub struct Nats {
    js: jetstream::Context,
    pipeline: String,
    part: PartitionConfig,
}

impl Nats {
    /// Connect to `url` and ensure a work-queue stream for each output edge (`outputs` are
    /// the ids of nodes that produce a stream: sources and transforms). Idempotent, so a
    /// restart or another worker reuses the existing streams. `part` must be valid (see
    /// [`PartitionConfig::validate`]); the caller checks it so a bad shape fails before any
    /// network I/O.
    pub async fn connect(
        url: &str,
        pipeline: &str,
        outputs: &[String],
        part: PartitionConfig,
    ) -> Result<Self> {
        // `retry_on_initial_connect` returns a client that connects in the background, so a
        // NATS that is not up yet at startup is waited for rather than a fatal error.
        let client = ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(url)
            .await
            .with_context(|| format!("connecting to NATS at {url}"))?;
        let js = jetstream::new(client);
        // Provisioning needs the connection, so retry until NATS is reachable; the process
        // then comes up cleanly once the server does.
        for out in outputs {
            let mut attempt = 0;
            while let Err(e) = js.get_or_create_stream(stream_config(pipeline, out)).await {
                tracing::warn!(
                    stream = %stream_name(pipeline, out),
                    error = %e,
                    "provisioning NATS stream failed; retrying"
                );
                tokio::time::sleep(backoff(attempt)).await;
                attempt += 1;
            }
        }
        Ok(Self {
            js,
            pipeline: pipeline.to_string(),
            part,
        })
    }

    /// Claim this worker's index in the pipeline's ownership bucket, failing fast if another
    /// worker already holds it (ADR-0016). The returned guard renews the lease until dropped;
    /// dropping it releases the index. Every worker claims, including a lone `--workers 1`, so
    /// two processes sharing a `--name` also collide.
    pub async fn claim_worker_lease(&self) -> Result<WorkerLease> {
        let store = self.workers_kv().await?;
        let key = self.part.index.to_string();
        let value = Bytes::from(worker_id().into_bytes());

        let mut attempt = 0;
        loop {
            match store.create(&key, value.clone()).await {
                Ok(_) => break,
                Err(e) => {
                    // A present key means a live worker already owns this index (fatal); anything
                    // else (e.g. NATS briefly unreachable) is transient and retried.
                    if matches!(store.get(key.as_str()).await, Ok(Some(_))) {
                        bail!(
                            "worker-index {} is already held for pipeline `{}`: another worker \
                             has the same index (check --workers / --worker-index)",
                            self.part.index,
                            self.pipeline
                        );
                    }
                    tracing::warn!(error = %e, "claiming the worker lease failed; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        }

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        tokio::spawn(renew_lease(store, key, value, stop_rx));
        Ok(WorkerLease { _stop: stop_tx })
    }

    /// Open (creating if needed) the pipeline's worker-lease bucket. Its TTL is what lets a
    /// crashed worker's lease expire so a replacement can reclaim the index.
    async fn workers_kv(&self) -> Result<kv::Store> {
        let bucket = workers_bucket(&self.pipeline);
        if let Ok(store) = self.js.get_key_value(&bucket).await {
            return Ok(store);
        }
        match self
            .js
            .create_key_value(kv::Config {
                bucket: bucket.clone(),
                max_age: LEASE_TTL,
                ..Default::default()
            })
            .await
        {
            Ok(store) => Ok(store),
            // Lost a create race with another worker; the bucket now exists.
            Err(_) => self
                .js
                .get_key_value(&bucket)
                .await
                .context("opening the worker-lease bucket"),
        }
    }
}

impl Backend for Nats {
    fn producer(&mut self, id: &str, key: &KeySpec) -> Box<dyn Producer> {
        Box::new(NatsProducer {
            js: self.js.clone(),
            pipeline: self.pipeline.clone(),
            node: id.to_string(),
            key: key.clone(),
            partitions: self.part.partitions,
            rr: AtomicU32::new(0),
        })
    }

    fn consumer(&mut self, id: &str) -> Box<dyn Consumer> {
        Box::new(NatsConsumer {
            js: self.js.clone(),
            stream: stream_name(&self.pipeline, id),
            pipeline: self.pipeline.clone(),
            node: id.to_string(),
            partitions: self.part.owned(),
            streams: None,
            last: None,
        })
    }
}

/// Holds a worker's index lease for the life of a run (ADR-0016). Dropping it stops the renewal
/// and releases the index; if the process dies instead, the lease expires after [`LEASE_TTL`].
#[derive(Debug)]
pub struct WorkerLease {
    // Dropping the sender signals [`renew_lease`] to release the key and stop.
    _stop: tokio::sync::oneshot::Sender<()>,
}

/// Renew `key` every third of [`LEASE_TTL`] so the lease stays live, until `stop` fires (the guard
/// dropped), then delete the key to free the index immediately rather than waiting out the TTL.
async fn renew_lease(
    store: kv::Store,
    key: String,
    value: Bytes,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) {
    let mut tick = tokio::time::interval(LEASE_TTL / 3);
    tick.tick().await; // the immediate first tick; the key was just created
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = store.put(&key, value.clone()).await {
                    tracing::warn!(key = %key, error = %e, "renewing the worker lease failed");
                }
            }
            _ = &mut stop => {
                let _ = store.delete(&key).await;
                return;
            }
        }
    }
}

struct NatsProducer {
    js: jetstream::Context,
    pipeline: String,
    node: String,
    key: KeySpec,
    partitions: u32,
    /// Round-robin cursor for unkeyed edges, so their records spread evenly.
    rr: AtomicU32,
}

impl NatsProducer {
    /// One publish attempt: enqueue and await the JetStream ack (record durably stored).
    /// Takes the encoded payload by value as `Bytes` so a retry re-sends the same buffer
    /// with only a cheap refcount bump, not a re-copy per attempt.
    async fn publish(&self, subject: &str, payload: Bytes) -> Result<()> {
        self.js
            .publish(subject.to_string(), payload)
            .await
            .context("publishing to NATS")?
            .await
            .context("awaiting publish ack")?;
        Ok(())
    }
}

#[async_trait]
impl Producer for NatsProducer {
    async fn send(&self, rec: Record) -> Result<()> {
        let p = route(&self.key, &rec, self.partitions, &self.rr);
        let subj = subject(&self.pipeline, &self.node, p);
        let payload = encode(&rec)?;
        // Retry through a NATS outage rather than failing the node: the client reconnects
        // underneath, so this back-pressures the pipeline until the publish is durably acked.
        // At-least-once (ADR-0015) makes a duplicate from an uncertain ack acceptable.
        let mut attempt = 0;
        loop {
            match self.publish(&subj, payload.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(subject = %subj, error = %e, "NATS publish failed; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

/// One partition's pull stream. Boxed and pinned so a [`StreamMap`] can own several and poll
/// them together.
type PartStream = Pin<Box<pull::Stream>>;

struct NatsConsumer {
    js: jetstream::Context,
    stream: String,
    pipeline: String,
    node: String,
    /// The partitions this worker owns for this edge.
    partitions: Vec<u32>,
    /// The owned partitions' message streams, merged and bound lazily on first `recv` so
    /// `consumer()` stays cheap.
    streams: Option<StreamMap<u32, PartStream>>,
    /// The previous message, acked on the next `recv` once the node has processed it
    /// (ack-after-processing; ADR-0015). Acking is per-message, so tracking one is correct
    /// no matter which partition it came from.
    last: Option<jetstream::Message>,
}

impl NatsConsumer {
    /// Bind (creating if needed) one durable pull consumer per owned partition and merge
    /// their message streams.
    async fn bind(&self) -> Result<StreamMap<u32, PartStream>> {
        let stream = self
            .js
            .get_stream(&self.stream)
            .await
            .context("getting stream")?;
        let mut map = StreamMap::new();
        for &p in &self.partitions {
            let durable = durable_name(&self.pipeline, &self.node, p);
            let filter = subject(&self.pipeline, &self.node, p);
            let consumer = stream
                .get_or_create_consumer(&durable, consumer_config(&durable, filter))
                .await
                .context("getting consumer")?;
            let messages = consumer
                .messages()
                .await
                .context("opening message stream")?;
            map.insert(p, Box::pin(messages));
        }
        Ok(map)
    }
}

#[async_trait]
impl Consumer for NatsConsumer {
    async fn recv(&mut self) -> Option<Record> {
        let mut attempt = 0;
        loop {
            if self.streams.is_none() {
                match self.bind().await {
                    Ok(m) => {
                        self.streams = Some(m);
                        attempt = 0;
                    }
                    Err(e) => {
                        tracing::warn!(stream = %self.stream, error = %e, "binding NATS consumer failed; retrying");
                        tokio::time::sleep(backoff(attempt)).await;
                        attempt += 1;
                        continue;
                    }
                }
            }
            // The node has finished with the previous record, so ack it before the next pull.
            if let Some(prev) = self.last.take() {
                let _ = prev.ack().await;
            }
            match self.streams.as_mut().expect("just bound").next().await {
                Some((_p, Ok(msg))) => match decode(&msg.payload) {
                    Ok(rec) => {
                        self.last = Some(msg);
                        return Some(rec);
                    }
                    // An undecodable record is a bug, not an outage: ack and skip it rather
                    // than wedge the stream on it forever.
                    Err(e) => {
                        tracing::error!(error = %e, "dropping an undecodable NATS record");
                        let _ = msg.ack().await;
                    }
                },
                // A transient error (e.g. a reconnect) - rebind all partitions and keep
                // consuming rather than reporting end-of-stream, which would stop the node.
                Some((_p, Err(e))) => {
                    tracing::warn!(stream = %self.stream, error = %e, "NATS consume error; rebinding");
                    self.streams = None;
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
                // Every owned partition's durable stream ended; rebind.
                None => {
                    self.streams = None;
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::Attrs;

    #[test]
    fn names_are_derived_consistently() {
        assert_eq!(subject("latency", "w", 3), "hr.latency.w.3");
        assert_eq!(wildcard_subject("latency", "w"), "hr.latency.w.*");
        // Stream and consumer names swap the subject's dots for underscores (JetStream
        // forbids dots in them).
        assert_eq!(stream_name("latency", "w"), "hr_latency_w");
        assert_eq!(durable_name("latency", "w", 3), "hr_latency_w_3_sink");
        assert_eq!(workers_bucket("latency"), "hr_latency_workers");
    }

    #[test]
    fn stream_config_is_a_work_queue_over_the_partition_wildcard() {
        let cfg = stream_config("latency", "w");
        assert_eq!(cfg.name, "hr_latency_w");
        assert_eq!(cfg.subjects, vec!["hr.latency.w.*".to_string()]);
        assert!(matches!(cfg.retention, stream::RetentionPolicy::WorkQueue));
    }

    #[test]
    fn consumer_config_filters_to_one_partition_with_explicit_acks() {
        let cfg = consumer_config("hr_latency_w_3_sink", "hr.latency.w.3".to_string());
        assert_eq!(cfg.durable_name.as_deref(), Some("hr_latency_w_3_sink"));
        assert_eq!(cfg.filter_subject, "hr.latency.w.3");
        assert!(matches!(
            cfg.ack_policy,
            jetstream::consumer::AckPolicy::Explicit
        ));
    }

    #[test]
    fn partition_is_deterministic_and_in_range() {
        let key = key_bytes(&svc_rec("checkout"), &["service.name".into()]);
        let p = partition(&key, 12);
        assert!(p < 12);
        // Same key, same partition - the property routing relies on.
        assert_eq!(partition(&key, 12), p);
        // Every key lands in range for a range of partition counts.
        for name in ["checkout", "cart", "search", "", "a-very-long-service-name"] {
            let k = key_bytes(&svc_rec(name), &["service.name".into()]);
            for n in [1u32, 3, 7, 12, 64] {
                assert!(partition(&k, n) < n);
            }
        }
    }

    #[test]
    fn key_bytes_is_typed_order_sensitive_and_distinguishes_absence() {
        let by = |r: &Record, f: &[&str]| {
            key_bytes(r, &f.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        };
        // Empty group_by (global aggregation) has an empty, so constant, key.
        assert!(by(&svc_rec("checkout"), &[]).is_empty());
        // Types are distinct: Int(1) must not encode like Str("1").
        let int_rec = one("k", AttrValue::Int(1));
        let str_rec = one("k", AttrValue::Str("1".into()));
        assert_ne!(by(&int_rec, &["k"]), by(&str_rec, &["k"]));
        // An absent field is distinct from any present value, and from a different absence.
        assert_ne!(by(&svc_rec("checkout"), &["k"]), by(&int_rec, &["k"]));
        // Field order matters, so co-partitioning depends on a stable declared order.
        let ab = one_of(&[("a", AttrValue::Int(1)), ("b", AttrValue::Int(2))]);
        assert_ne!(by(&ab, &["a", "b"]), by(&ab, &["b", "a"]));
    }

    #[test]
    fn route_hashes_keyed_edges_and_round_robins_unkeyed() {
        let rec = svc_rec("checkout");
        // Keyed: deterministic, and equal to the partition of the key bytes.
        let keyed = KeySpec::Keyed(vec!["service.name".into()]);
        let rr = AtomicU32::new(0);
        let want = partition(&key_bytes(&rec, &["service.name".into()]), 6);
        assert_eq!(route(&keyed, &rec, 6, &rr), want);
        assert_eq!(
            route(&keyed, &rec, 6, &rr),
            want,
            "same key, same partition"
        );
        // Unkeyed: round-robins across the partitions, wrapping at the count.
        let rr = AtomicU32::new(0);
        let seq: Vec<u32> = (0..7)
            .map(|_| route(&KeySpec::Unkeyed, &rec, 3, &rr))
            .collect();
        assert_eq!(seq, vec![0, 1, 2, 0, 1, 2, 0]);
    }

    #[test]
    fn owned_partitions_split_disjointly_and_cover_all() {
        let (partitions, workers) = (12u32, 3u32);
        let mut union = Vec::new();
        for index in 0..workers {
            let owned = PartitionConfig {
                partitions,
                workers,
                index,
            }
            .owned();
            assert!(owned.iter().all(|p| p % workers == index));
            union.extend(owned);
        }
        union.sort_unstable();
        assert_eq!(union, (0..partitions).collect::<Vec<_>>());
        // A single worker owns everything (the single-worker case).
        assert_eq!(
            PartitionConfig {
                partitions: 12,
                workers: 1,
                index: 0
            }
            .owned()
            .len(),
            12
        );
    }

    #[test]
    fn partition_config_validate_rejects_bad_shapes() {
        let ok = PartitionConfig {
            partitions: 12,
            workers: 3,
            index: 2,
        };
        assert!(ok.validate().is_ok());
        // index out of range, too many workers, and empty counts are all rejected.
        for bad in [
            PartitionConfig {
                partitions: 12,
                workers: 3,
                index: 3,
            },
            PartitionConfig {
                partitions: 4,
                workers: 8,
                index: 0,
            },
            PartitionConfig {
                partitions: 0,
                workers: 1,
                index: 0,
            },
            PartitionConfig {
                partitions: 12,
                workers: 0,
                index: 0,
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn backoff_grows_then_caps_at_5s() {
        assert_eq!(backoff(0), Duration::from_millis(100));
        assert_eq!(backoff(1), Duration::from_millis(200));
        assert_eq!(backoff(3), Duration::from_millis(800));
        // Capped, and monotonic past the cap.
        assert_eq!(backoff(6), Duration::from_millis(5_000));
        assert_eq!(backoff(50), Duration::from_millis(5_000));
    }

    #[test]
    fn record_round_trips_through_messagepack() {
        let rec = svc_rec("checkout");
        let back = decode(encode(&rec).unwrap().as_ref()).unwrap();
        assert_eq!(back.name, "latency");
        assert_eq!(
            back.attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
    }

    fn svc_rec(svc: &str) -> Record {
        one("service.name", AttrValue::Str(svc.into()))
    }

    fn one(key: &str, value: AttrValue) -> Record {
        one_of(&[(key, value)])
    }

    fn one_of(attrs: &[(&str, AttrValue)]) -> Record {
        Record {
            ts_nanos: 42,
            start_ts_nanos: Some(0),
            resource: Attrs::new(),
            scope: None,
            name: "latency".into(),
            value: 3.5,
            attrs: attrs
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }
}
