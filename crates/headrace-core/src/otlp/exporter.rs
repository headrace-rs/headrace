//! OTLP/gRPC metrics exporter: batch Records and send them to an OTLP endpoint. A connect or
//! export failure is not fatal - the exporter retries with capped backoff, and because it
//! stops draining its input while retrying, the bounded input channel back-pressures upstream.
//! So a transient collector outage stalls the pipeline rather than stopping it (ADR-0017).
//! At-least-once: a retried export can duplicate on an uncertain ack.

use crate::backend::Consumer;
use crate::metrics::NodeMetrics;
use crate::record::Record;
use anyhow::Result;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use std::time::Duration;
use tonic::transport::Channel;

const MAX_BATCH: usize = 512;
const FLUSH: Duration = Duration::from_millis(200);

/// Exponential backoff for the retry loops, capped at 5s: 100ms, 200ms, ... 5s. Mirrors the
/// NATS backend; ADR-0017 defers a shared helper until a third caller appears.
fn backoff(attempt: u32) -> Duration {
    let ms = 100u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(ms.min(5_000))
}

/// Connect to `endpoint`, retrying with backoff until it answers. Never fails: a collector that
/// is not up yet is waited for, not treated as fatal (ADR-0017).
async fn connect(endpoint: &str) -> MetricsServiceClient<Channel> {
    let mut attempt = 0;
    loop {
        match MetricsServiceClient::connect(endpoint.to_string()).await {
            Ok(client) => return client,
            Err(e) => {
                tracing::warn!(endpoint, error = %e, "connecting to OTLP endpoint failed; retrying");
                tokio::time::sleep(backoff(attempt)).await;
                attempt += 1;
            }
        }
    }
}

/// Drain Records, batch them, and export to the OTLP endpoint until the input closes.
pub async fn run(endpoint: String, mut rx: Box<dyn Consumer>, nm: NodeMetrics) -> Result<()> {
    let mut client = connect(&endpoint).await;
    let mut buf: Vec<Record> = Vec::new();
    let mut ticker = tokio::time::interval(FLUSH);
    ticker.tick().await; // drop the immediate first tick
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(rec) => {
                    buf.push(rec);
                    if buf.len() >= MAX_BATCH {
                        flush(&mut client, &mut buf, &nm).await;
                    }
                }
                None => break,
            },
            _ = ticker.tick() => flush(&mut client, &mut buf, &nm).await,
        }
    }
    flush(&mut client, &mut buf, &nm).await;
    Ok(())
}

/// Export the batch, retrying with backoff until it succeeds. While retrying, the caller stops
/// draining its input, so the bounded channel back-pressures upstream (ADR-0017). The same
/// request is re-sent each attempt, so a duplicate is possible on an uncertain ack.
async fn flush(
    client: &mut MetricsServiceClient<Channel>,
    buf: &mut Vec<Record>,
    nm: &NodeMetrics,
) {
    if buf.is_empty() {
        return;
    }
    let count = buf.len();
    let req = super::convert::encode(std::mem::take(buf));
    let mut attempt = 0;
    loop {
        match client.export(req.clone()).await {
            Ok(_) => {
                for _ in 0..count {
                    nm.out();
                }
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "OTLP export failed; retrying");
                tokio::time::sleep(backoff(attempt)).await;
                attempt += 1;
            }
        }
    }
}
