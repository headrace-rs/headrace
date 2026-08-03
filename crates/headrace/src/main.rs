mod metrics;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use headrace_ir::Pipeline;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "headrace",
    version,
    about = "OTel-native stateful stream processing"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
    /// Log filter (e.g. info, headrace_core=debug).
    #[arg(long, default_value = "info", global = true)]
    log: String,
    /// Self-telemetry exporter for headrace's own metrics (stdout mode interleaves with data).
    #[arg(long, value_enum, default_value = "off", global = true)]
    metrics: metrics::Mode,
    /// OTLP endpoint for `--metrics otlp` (else OTEL_EXPORTER_OTLP_ENDPOINT / default).
    #[arg(long, global = true)]
    otlp_endpoint: Option<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a pipeline until Ctrl-C.
    Run { file: PathBuf },
    /// Parse and statically check a pipeline.
    Validate { file: PathBuf },
    /// Print the IR JSON Schema.
    Schema,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // Logs go to stderr so stdout carries only the pipeline's data (the stdout sink).
    tracing_subscriber::fmt()
        .with_env_filter(cli.log.clone())
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    match cli.cmd {
        Cmd::Run { file } => {
            let pipeline = load(&file)?;
            let telemetry = metrics::init(cli.metrics, cli.otlp_endpoint.clone())?;
            let recorder: headrace_core::SharedMetrics = match &telemetry {
                Some(t) => t.metrics.clone(),
                None => Arc::new(headrace_core::NoopMetrics),
            };
            let result = headrace_core::run(pipeline, recorder).await;
            if let Some(t) = telemetry {
                t.shutdown();
            }
            result
        }
        Cmd::Validate { file } => {
            headrace_core::validate(&load(&file)?)?;
            println!("ok");
            Ok(())
        }
        Cmd::Schema => {
            println!("{}", headrace_ir::json_schema());
            Ok(())
        }
    }
}

fn load(file: &PathBuf) -> Result<Pipeline> {
    let text = std::fs::read_to_string(file).with_context(|| format!("reading {file:?}"))?;
    serde_yaml::from_str(&text).with_context(|| format!("parsing {file:?}"))
}
