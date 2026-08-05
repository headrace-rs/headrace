//! OTLP/gRPC metrics receiver: decode incoming metrics to Records and push them downstream.

use crate::backend::Producer;
use crate::metrics::NodeMetrics;
use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::{MetricsService, MetricsServiceServer},
};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

use super::normalize::Normalizer;

#[derive(Clone)]
struct Service {
    tx: Arc<dyn Producer>,
    nm: NodeMetrics,
    // Shared across the per-connection clones tonic makes, so cumulative-to-delta state
    // is one series history for the whole receiver, not one per connection.
    norm: Arc<Mutex<Normalizer>>,
}

#[tonic::async_trait]
impl MetricsService for Service {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        // Decode (and normalize) under the lock; hold nothing across the sends below.
        let records = {
            let mut norm = self.norm.lock().expect("normalizer mutex poisoned");
            super::convert::decode(request.into_inner(), &mut norm)
        };
        for rec in records {
            if self.tx.send(None, rec).await.is_err() {
                return Err(Status::unavailable("pipeline closed"));
            }
            self.nm.out();
        }
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

/// Serve the OTLP metrics gRPC endpoint on `listen`, pushing each datapoint downstream as a Record.
pub async fn run(listen: String, tx: Box<dyn Producer>, nm: NodeMetrics) -> Result<()> {
    let addr: SocketAddr = listen
        .parse()
        .with_context(|| format!("invalid OTLP listen address `{listen}`"))?;
    let service = Service {
        tx: Arc::from(tx),
        nm,
        norm: Arc::new(Mutex::new(Normalizer::default())),
    };
    tracing::info!(%addr, "OTLP receiver listening");
    tonic::transport::Server::builder()
        .add_service(MetricsServiceServer::new(service))
        .serve_with_shutdown(addr, crate::runtime::shutdown_signal())
        .await
        .context("OTLP receiver")?;
    Ok(())
}
