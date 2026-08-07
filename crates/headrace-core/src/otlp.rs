//! OTLP source (gRPC receiver) and sink (gRPC exporter), plus Record <-> OTLP conversion.
//! Enabled by the `otlp` feature. Metrics only for now (Gauge and Sum datapoints).

pub mod convert;
pub mod exporter;
pub mod normalize;
pub mod receiver;
