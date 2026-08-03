mod metrics;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use headrace_core::backend::InProcess;
use headrace_ir::Pipeline;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

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
    /// Log output format.
    #[arg(long, value_enum, default_value = "text", global = true)]
    log_format: LogFormat,
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
    // stderr keeps stdout pure for the stdout sink; the guard flushes the non-blocking
    // writer on exit, so it must live for the whole command.
    let _guard = init_tracing(&cli.log, cli.log_format);

    match cli.cmd {
        Cmd::Run { file } => {
            let pipeline = load(&file)?;
            let telemetry = metrics::init(cli.metrics, cli.otlp_endpoint.clone())?;
            let recorder: headrace_core::SharedMetrics = match &telemetry {
                Some(t) => t.metrics.clone(),
                None => Arc::new(headrace_core::NoopMetrics),
            };
            let result = headrace_core::run(pipeline, InProcess::default(), recorder).await;
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

/// Initialize logging to a non-blocking stderr writer. Returns the writer's guard, which must
/// be held for the process lifetime (its drop flushes buffered logs).
fn init_tracing(filter: &str, format: LogFormat) -> tracing_appender::non_blocking::WorkerGuard {
    let (writer, guard) = tracing_appender::non_blocking(std::io::stderr());
    let base = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .with_target(false)
        .with_ansi(false)
        .with_writer(writer);
    match format {
        LogFormat::Json => base.json().flatten_event(true).init(),
        LogFormat::Text => base.init(),
    }
    guard
}

fn load(file: &PathBuf) -> Result<Pipeline> {
    let text = std::fs::read_to_string(file).with_context(|| format!("reading {file:?}"))?;
    serde_yaml::from_str(&text).with_context(|| format!("parsing {file:?}"))
}
