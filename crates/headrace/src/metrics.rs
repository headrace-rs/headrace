//! OTel-backed [`Metrics`] for the binary. Kept out of `headrace-core` so the core carries
//! no OpenTelemetry dependency - the SDK lives only here, behind the `Metrics` boundary.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::ValueEnum;
use headrace_core::SharedMetrics;
use headrace_core::metrics::{DropReason, Metrics, NodeKind, NodeRecorder};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};
use opentelemetry_otlp::WithExportConfig; // brings `with_endpoint` onto the OTLP builder
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

/// Where headrace's self-telemetry goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// No self-telemetry (default).
    Off,
    /// Periodically print metrics to stderr - for debugging.
    Stdout,
    /// Export metrics over OTLP/gRPC to a collector.
    Otlp,
}

/// Live telemetry: the shared recorder plus the provider to flush on shutdown.
pub struct Telemetry {
    pub metrics: SharedMetrics,
    provider: SdkMeterProvider,
}

impl Telemetry {
    /// Flush and stop the exporter. Call after the pipeline returns.
    pub fn shutdown(self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!("metrics shutdown: {e}");
        }
    }
}

/// Build telemetry for `mode`; `None` for `Off`. `endpoint` overrides the OTLP target
/// (otherwise the standard `OTEL_EXPORTER_OTLP_ENDPOINT` / default applies).
pub fn init(mode: Mode, endpoint: Option<String>) -> Result<Option<Telemetry>> {
    let resource = Resource::builder().with_service_name("headrace").build();
    let provider = match mode {
        Mode::Off => return Ok(None),
        // NB: the 0.32 stdout exporter is hardcoded to stdout, so metric dumps interleave
        // with the stdout sink's data. Debug convenience only - use OTLP for clean output.
        Mode::Stdout => SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
            .build(),
        Mode::Otlp => {
            let mut builder = opentelemetry_otlp::MetricExporter::builder().with_tonic();
            if let Some(ep) = endpoint {
                builder = builder.with_endpoint(ep);
            }
            let exporter = builder.build().context("build OTLP metric exporter")?;
            SdkMeterProvider::builder()
                .with_resource(resource)
                .with_periodic_exporter(exporter)
                .build()
        }
    };
    let metrics: SharedMetrics = Arc::new(OtelMetrics::new(&provider.meter("headrace")));
    Ok(Some(Telemetry { metrics, provider }))
}

/// Holds the OTel instruments; hands out per-node recorders that pre-bind attributes.
struct OtelMetrics {
    records_out: Counter<u64>,
    records_dropped: Counter<u64>,
    window_flushes: Counter<u64>,
    window_groups: Histogram<u64>,
    node_errors: Counter<u64>,
    wasm_memory: Histogram<u64>,
}

impl OtelMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            records_out: meter
                .u64_counter("headrace.records.out")
                .with_description("Records emitted/forwarded by a node")
                .build(),
            records_dropped: meter
                .u64_counter("headrace.records.dropped")
                .with_description("Records dropped, labeled by reason")
                .build(),
            window_flushes: meter
                .u64_counter("headrace.window.flushes")
                .with_description("Window flush events")
                .build(),
            window_groups: meter
                .u64_histogram("headrace.window.groups")
                .with_description("Aggregate groups emitted per window flush")
                .build(),
            node_errors: meter
                .u64_counter("headrace.node.errors")
                .with_description("Node tasks that terminated with an error")
                .build(),
            wasm_memory: meter
                .u64_histogram("headrace.wasm.memory.bytes")
                .with_description("A wasm module's linear-memory size in bytes")
                .build(),
        }
    }
}

impl Metrics for OtelMetrics {
    fn node(&self, node: &str, kind: NodeKind) -> Arc<dyn NodeRecorder> {
        Arc::new(OtelNodeRecorder {
            attrs: [
                KeyValue::new("node", node.to_string()),
                KeyValue::new("kind", kind.as_str()),
            ],
            records_out: self.records_out.clone(),
            records_dropped: self.records_dropped.clone(),
            window_flushes: self.window_flushes.clone(),
            window_groups: self.window_groups.clone(),
            node_errors: self.node_errors.clone(),
            wasm_memory: self.wasm_memory.clone(),
        })
    }
}

/// One node's recorder: attribute set computed once here, instruments cloned (cheap,
/// `Arc`-backed). The per-record `record_out`/`window_flushed` paths allocate nothing;
/// `record_dropped` clones the two base labels to append a `reason`.
struct OtelNodeRecorder {
    attrs: [KeyValue; 2], // [node, kind]
    records_out: Counter<u64>,
    records_dropped: Counter<u64>,
    window_flushes: Counter<u64>,
    window_groups: Histogram<u64>,
    node_errors: Counter<u64>,
    wasm_memory: Histogram<u64>,
}

impl NodeRecorder for OtelNodeRecorder {
    fn record_out(&self) {
        self.records_out.add(1, &self.attrs);
    }
    fn record_dropped(&self, n: u64, reason: DropReason) {
        self.records_dropped.add(
            n,
            &[
                self.attrs[0].clone(),
                self.attrs[1].clone(),
                KeyValue::new("reason", reason.as_str()),
            ],
        );
    }
    fn window_flushed(&self, groups: u64) {
        let node_only = &self.attrs[..1]; // just the node label
        self.window_flushes.add(1, node_only);
        self.window_groups.record(groups, node_only);
    }
    fn node_error(&self) {
        self.node_errors.add(1, &self.attrs);
    }
    fn wasm_memory(&self, bytes: u64) {
        self.wasm_memory.record(bytes, &self.attrs);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn off_mode_produces_no_telemetry() {
        assert!(init(Mode::Off, None).unwrap().is_none());
    }

    #[test]
    fn stdout_mode_records_through_every_instrument() {
        // Builds a real provider (stdout exporter, no network) and drives every recorder
        // path: per-node binding, all four counters/histogram, then a clean shutdown.
        let telemetry = init(Mode::Stdout, None).unwrap().expect("stdout telemetry");
        let rollup = telemetry.metrics.node("rollup", NodeKind::Window);
        rollup.record_out();
        rollup.record_dropped(2, DropReason::Filtered);
        rollup.record_dropped(2, DropReason::Invalid);
        rollup.record_dropped(1, DropReason::Late);
        rollup.record_dropped(1, DropReason::Incomplete);
        rollup.record_dropped(4, DropReason::Capped);
        rollup.window_flushed(3);
        rollup.node_error();
        rollup.wasm_memory(1 << 20);
        telemetry.metrics.node("gen", NodeKind::Source).record_out();
        telemetry.shutdown();
    }
}
