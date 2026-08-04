//! OTLP/gRPC metrics exporter: batch Records and send them to an OTLP endpoint.

use crate::backend::Consumer;
use crate::metrics::NodeMetrics;
use crate::record::Record;
use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
use std::time::Duration;
use tonic::transport::Channel;

const MAX_BATCH: usize = 512;
const FLUSH: Duration = Duration::from_millis(200);

/// Drain Records, batch them, and export to the OTLP endpoint until the input closes.
pub async fn run(endpoint: String, mut rx: Box<dyn Consumer>, nm: NodeMetrics) -> Result<()> {
    let mut client = MetricsServiceClient::connect(endpoint.clone())
        .await
        .with_context(|| format!("connecting to OTLP endpoint `{endpoint}`"))?;
    let mut buf: Vec<Record> = Vec::new();
    let mut ticker = tokio::time::interval(FLUSH);
    ticker.tick().await; // drop the immediate first tick
    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(rec) => {
                    buf.push(rec);
                    if buf.len() >= MAX_BATCH {
                        flush(&mut client, &mut buf, &nm).await?;
                    }
                }
                None => break,
            },
            _ = ticker.tick() => flush(&mut client, &mut buf, &nm).await?,
        }
    }
    flush(&mut client, &mut buf, &nm).await
}

async fn flush(
    client: &mut MetricsServiceClient<Channel>,
    buf: &mut Vec<Record>,
    nm: &NodeMetrics,
) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let count = buf.len();
    let req = super::convert::encode(std::mem::take(buf));
    client.export(req).await.context("OTLP export")?;
    for _ in 0..count {
        nm.out();
    }
    Ok(())
}
