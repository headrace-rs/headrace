//! NATS JetStream backend (ADR-0003, ADR-0015): a durable, back-pressured edge between
//! nodes for the scaled deployment. Stage 1 (here) is a single worker over JetStream:
//! one work-queue stream per node output, a durable pull consumer per input, records on
//! the wire as MessagePack, and at-least-once delivery via ack-after-processing. Static
//! partitioning across workers is stage 2.
//!
//! The deterministic parts - subject/stream naming, the codec, the stream and consumer
//! config - are pure functions, unit-tested below. Only the thin async glue (connect,
//! publish, pull) needs a live server, and it is covered by the `nats_e2e` integration test.

use crate::backend::{Backend, Consumer, KeySpec, Producer};
use crate::record::Record;
use anyhow::{Context, Result};
use async_nats::ConnectOptions;
use async_nats::jetstream::{self, consumer::pull, stream};
use async_trait::async_trait;
use std::time::Duration;
use tokio_stream::StreamExt;

/// The subject a node's output records are published to. `<pipeline>` namespaces subjects so
/// many pipelines can share one cluster.
fn subject(pipeline: &str, node: &str) -> String {
    format!("hr.{pipeline}.{node}")
}

/// The JetStream stream name for a node output. Stream names cannot contain `.`, so the
/// subject's dots become underscores.
fn stream_name(pipeline: &str, node: &str) -> String {
    format!("hr_{pipeline}_{node}")
}

/// The durable pull-consumer name for a node output (its single downstream reader).
fn durable_name(pipeline: &str, node: &str) -> String {
    format!("{}_sink", stream_name(pipeline, node))
}

/// The work-queue stream config for a node output: a record leaves the stream once its
/// single consumer acks it, matching the runtime's one-consumer-per-output rule.
fn stream_config(pipeline: &str, node: &str) -> stream::Config {
    stream::Config {
        name: stream_name(pipeline, node),
        subjects: vec![subject(pipeline, node)],
        retention: stream::RetentionPolicy::WorkQueue,
        ..Default::default()
    }
}

/// The durable pull-consumer config. Explicit acks drive the ack-after-processing model.
fn consumer_config(durable: &str) -> pull::Config {
    pull::Config {
        durable_name: Some(durable.to_string()),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ..Default::default()
    }
}

fn encode(rec: &Record) -> Result<Vec<u8>> {
    rmp_serde::to_vec(rec).context("encoding record")
}

fn decode(payload: &[u8]) -> Result<Record> {
    rmp_serde::from_slice(payload).context("decoding record")
}

/// Exponential backoff for a retry loop, capped at 5s: 100ms, 200ms, 400ms, ... 5s, 5s.
fn backoff(attempt: u32) -> Duration {
    let ms = 100u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(ms.min(5_000))
}

/// A NATS JetStream backend. Cheap to make handles from; the network connection is made
/// once in [`Nats::connect`], which also provisions a stream per output.
pub struct Nats {
    js: jetstream::Context,
    pipeline: String,
}

impl Nats {
    /// Connect to `url` and ensure a work-queue stream for each output edge (`outputs` are
    /// the ids of nodes that produce a stream: sources and transforms). Idempotent, so a
    /// restart or a second worker reuses the existing streams.
    pub async fn connect(url: &str, pipeline: &str, outputs: &[String]) -> Result<Self> {
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
        })
    }
}

impl Backend for Nats {
    fn producer(&mut self, id: &str, _key: &KeySpec) -> Box<dyn Producer> {
        // Stage 1 is single-partition; the key drives routing in stage 2.
        Box::new(NatsProducer {
            js: self.js.clone(),
            subject: subject(&self.pipeline, id),
        })
    }

    fn consumer(&mut self, id: &str) -> Box<dyn Consumer> {
        Box::new(NatsConsumer {
            js: self.js.clone(),
            stream: stream_name(&self.pipeline, id),
            durable: durable_name(&self.pipeline, id),
            messages: None,
            last: None,
        })
    }
}

struct NatsProducer {
    js: jetstream::Context,
    subject: String,
}

impl NatsProducer {
    /// One publish attempt: enqueue and await the JetStream ack (record durably stored).
    async fn publish(&self, payload: &[u8]) -> Result<()> {
        self.js
            .publish(self.subject.clone(), payload.to_vec().into())
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
        let payload = encode(&rec)?;
        // Retry through a NATS outage rather than failing the node: the client reconnects
        // underneath, so this back-pressures the pipeline until the publish is durably acked.
        // At-least-once (ADR-0015) makes a duplicate from an uncertain ack acceptable.
        let mut attempt = 0;
        loop {
            match self.publish(&payload).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(subject = %self.subject, error = %e, "NATS publish failed; retrying");
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

struct NatsConsumer {
    js: jetstream::Context,
    stream: String,
    durable: String,
    /// The pull message stream, bound lazily on first `recv` so `consumer()` stays cheap.
    messages: Option<pull::Stream>,
    /// The previous message, acked on the next `recv` once the node has processed it
    /// (ack-after-processing; ADR-0015).
    last: Option<jetstream::Message>,
}

impl NatsConsumer {
    /// Bind (creating if needed) the durable pull consumer and open its message stream.
    async fn bind(&self) -> Result<pull::Stream> {
        let stream = self
            .js
            .get_stream(&self.stream)
            .await
            .context("getting stream")?;
        let consumer = stream
            .get_or_create_consumer(&self.durable, consumer_config(&self.durable))
            .await
            .context("getting consumer")?;
        consumer.messages().await.context("opening message stream")
    }
}

#[async_trait]
impl Consumer for NatsConsumer {
    async fn recv(&mut self) -> Option<Record> {
        let mut attempt = 0;
        loop {
            if self.messages.is_none() {
                match self.bind().await {
                    Ok(m) => {
                        self.messages = Some(m);
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
            match self.messages.as_mut().expect("just bound").next().await {
                Some(Ok(msg)) => match decode(&msg.payload) {
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
                // A transient error (e.g. a reconnect) - rebind and keep consuming rather
                // than reporting end-of-stream, which would stop the node.
                Some(Err(e)) => {
                    tracing::warn!(stream = %self.stream, error = %e, "NATS consume error; rebinding");
                    self.messages = None;
                    tokio::time::sleep(backoff(attempt)).await;
                    attempt += 1;
                }
                // A durable pull stream is open-ended; if it ends, rebind.
                None => {
                    self.messages = None;
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
    use crate::record::{AttrValue, Attrs};

    #[test]
    fn names_are_derived_consistently() {
        assert_eq!(subject("latency", "w"), "hr.latency.w");
        // Stream and consumer names swap the subject's dots for underscores (JetStream
        // forbids dots in them).
        assert_eq!(stream_name("latency", "w"), "hr_latency_w");
        assert_eq!(durable_name("latency", "w"), "hr_latency_w_sink");
    }

    #[test]
    fn stream_config_is_a_work_queue_over_the_node_subject() {
        let cfg = stream_config("latency", "w");
        assert_eq!(cfg.name, "hr_latency_w");
        assert_eq!(cfg.subjects, vec!["hr.latency.w".to_string()]);
        assert!(matches!(cfg.retention, stream::RetentionPolicy::WorkQueue));
    }

    #[test]
    fn consumer_config_uses_explicit_acks() {
        let cfg = consumer_config("hr_latency_w_sink");
        assert_eq!(cfg.durable_name.as_deref(), Some("hr_latency_w_sink"));
        assert!(matches!(
            cfg.ack_policy,
            jetstream::consumer::AckPolicy::Explicit
        ));
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
        let mut attrs = Attrs::new();
        attrs.insert("service.name".into(), AttrValue::Str("checkout".into()));
        let rec = Record {
            ts_nanos: 42,
            start_ts_nanos: Some(0),
            resource: Attrs::new(),
            scope: None,
            name: "latency".into(),
            value: 3.5,
            attrs,
        };
        let back = decode(&encode(&rec).unwrap()).unwrap();
        assert_eq!(back.ts_nanos, 42);
        assert_eq!(back.name, "latency");
        assert_eq!(back.value, 3.5);
        assert_eq!(
            back.attrs.get("service.name"),
            Some(&AttrValue::Str("checkout".into()))
        );
    }
}
