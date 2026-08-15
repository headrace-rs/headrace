mod metrics;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use headrace_core::backend::InProcess;
use headrace_ir::Pipeline;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

/// Which backend carries records between nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum BackendKind {
    /// In-memory channels; a single self-contained process (default).
    #[value(name = "in-process")]
    InProcess,
    /// NATS JetStream: durable, partitioned edges for the scaled deployment.
    Nats,
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
    Run {
        file: PathBuf,
        /// Serve the state-inspection gRPC API on this address (e.g. 127.0.0.1:4318).
        /// Off by default; exposes raw node state, so bind a trusted network only.
        #[arg(long, value_name = "ADDR")]
        inspect_addr: Option<SocketAddr>,
        /// Record transport between nodes.
        #[arg(long, value_enum, default_value = "in-process")]
        backend: BackendKind,
        /// NATS server URL for `--backend nats` (e.g. nats://127.0.0.1:4222).
        #[arg(long, value_name = "URL")]
        nats_url: Option<String>,
        /// Pipeline name; namespaces the NATS subjects (default: the pipeline file stem).
        #[arg(long)]
        name: Option<String>,
        /// Partitions per edge for `--backend nats` (fixed key-groups; ADR-0015).
        #[arg(long, default_value_t = 12)]
        partitions: u32,
        /// Total workers sharing the partitions; this process is one of them.
        #[arg(long, default_value_t = 1)]
        workers: u32,
        /// This worker's index in `0..workers` (e.g. a StatefulSet ordinal).
        #[arg(long, env = "HEADRACE_WORKER_INDEX", default_value_t = 0)]
        worker_index: u32,
    },
    /// Parse and statically check a pipeline.
    Validate { file: PathBuf },
    /// Print the IR JSON Schema.
    Schema,
    /// Query a running pipeline's live state (needs `run --inspect-addr`).
    Inspect {
        /// Address of the pipeline's state server (e.g. 127.0.0.1:4318).
        addr: SocketAddr,
        /// Restrict to these node ids (repeatable); omit for all stateful nodes.
        #[arg(long = "node", value_name = "ID")]
        node: Vec<String>,
        /// Stream snapshots as state changes, instead of a one-shot query. Ctrl-C to stop.
        #[arg(long)]
        watch: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    // stderr keeps stdout pure for the stdout sink; the guard flushes the non-blocking
    // writer on exit, so it must live for the whole command.
    let _guard = init_tracing(&cli.log, cli.log_format);

    match cli.cmd {
        Cmd::Run {
            file,
            inspect_addr,
            backend,
            nats_url,
            name,
            partitions,
            workers,
            worker_index,
        } => {
            let pipeline = load(&file)?;
            let telemetry = metrics::init(cli.metrics, cli.otlp_endpoint.clone())?;
            let recorder: headrace_core::SharedMetrics = match &telemetry {
                Some(t) => t.metrics.clone(),
                None => Arc::new(headrace_core::NoopMetrics),
            };
            let opts = headrace_core::RunOptions { inspect_addr };
            let nats = NatsOpts {
                url: nats_url,
                name,
                part: headrace_core::backend::PartitionConfig {
                    partitions,
                    workers,
                    index: worker_index,
                },
            };
            let result = run_pipeline(pipeline, backend, nats, &file, recorder, opts).await;
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
        Cmd::Inspect { addr, node, watch } => inspect(addr, node, watch).await,
    }
}

/// Query the `State` server at `addr` and print each node's open groups. `node` restricts to
/// specific ids; empty asks for all stateful nodes. With `watch`, stream snapshots as node
/// state changes until interrupted.
async fn inspect(addr: SocketAddr, node: Vec<String>, watch: bool) -> Result<()> {
    use headrace_proto::v1::state_client::StateClient;
    use headrace_proto::v1::{GetRequest, WatchRequest};

    let mut client = StateClient::connect(format!("http://{addr}"))
        .await
        .with_context(|| format!("connecting to the state server at {addr}"))?;
    if watch {
        let mut stream = client
            .watch(WatchRequest { node })
            .await
            .context("State.Watch request failed")?
            .into_inner();
        while let Some(node) = stream.message().await.context("watch stream error")? {
            print!("{}", render(std::slice::from_ref(&node)));
        }
    } else {
        let resp = client
            .get(GetRequest { node })
            .await
            .context("State.Get request failed")?;
        print!("{}", render(&resp.into_inner().nodes));
    }
    Ok(())
}

/// Render `State.Get` results as a plain, greppable table.
fn render(nodes: &[headrace_proto::v1::NodeState]) -> String {
    use std::fmt::Write;

    if nodes.is_empty() {
        return "no stateful nodes\n".to_string();
    }
    let mut out = String::new();
    for n in nodes {
        let _ = writeln!(out, "{} ({}) - {} group(s)", n.id, n.kind, n.groups.len());
        for g in &n.groups {
            let labels = join_pairs(g.labels.iter().map(|(k, v)| format!("{k}={v}")));
            let _ = write!(
                out,
                "  {labels}  window=[{},{})",
                g.window_start_nanos, g.window_end_nanos
            );
            if let Some(v) = g.value {
                let _ = write!(out, "  value={v}");
            }
            if !g.inputs.is_empty() {
                let inputs = join_pairs(g.inputs.iter().map(|(k, v)| format!("{k}={v}")));
                let _ = write!(out, "  inputs={{{inputs}}}");
            }
            let _ = writeln!(out, "  samples={}", g.samples);
        }
    }
    out
}

/// Sort `k=v` pairs for stable output (proto maps are unordered) and join them, or `-` when
/// there are none.
fn join_pairs(pairs: impl Iterator<Item = String>) -> String {
    let mut pairs: Vec<String> = pairs.collect();
    if pairs.is_empty() {
        return "-".to_string();
    }
    pairs.sort();
    pairs.join(",")
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
    serde_norway::from_str(&text).with_context(|| format!("parsing {file:?}"))
}

/// NATS backend options from the CLI, used only with `--backend nats`.
struct NatsOpts {
    url: Option<String>,
    name: Option<String>,
    part: headrace_core::backend::PartitionConfig,
}

/// Wire the pipeline onto the chosen backend and run it. `run` is generic over the backend,
/// so each arm monomorphizes its own concrete type.
async fn run_pipeline(
    pipeline: Pipeline,
    backend: BackendKind,
    nats: NatsOpts,
    file: &Path,
    recorder: headrace_core::SharedMetrics,
    opts: headrace_core::RunOptions,
) -> Result<()> {
    match backend {
        BackendKind::InProcess => {
            headrace_core::run(pipeline, InProcess::default(), recorder, opts).await
        }
        BackendKind::Nats => {
            nats.part.validate()?;
            let url = nats.url.context("--backend nats requires --nats-url")?;
            let name = nats.name.unwrap_or_else(|| pipeline_name(file));
            let outputs = output_ids(&pipeline);
            let nats =
                headrace_core::backend::Nats::connect(&url, &name, &outputs, nats.part).await?;
            // Hold the ownership lease for the run so a duplicate worker index fails fast
            // (ADR-0016); dropping it after `run` releases the index.
            let _lease = nats.claim_worker_lease().await?;
            headrace_core::run(pipeline, nats, recorder, opts).await
        }
    }
}

/// Ids of nodes that produce an output stream: sources and transforms (sinks are terminal).
fn output_ids(p: &Pipeline) -> Vec<String> {
    p.sources
        .iter()
        .map(|s| s.id().to_string())
        .chain(p.transforms.iter().map(|t| t.id().to_string()))
        .collect()
}

/// Pipeline name defaulted from the file stem (`examples/latency.yaml` -> `latency`).
fn pipeline_name(file: &Path) -> String {
    file.file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("headrace")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use headrace_proto::v1::{GroupState, NodeState};

    fn group(labels: &[(&str, &str)], value: Option<f64>, samples: u64) -> GroupState {
        GroupState {
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            window_start_nanos: 0,
            window_end_nanos: 5_000_000_000,
            value,
            inputs: Default::default(),
            samples,
        }
    }

    #[test]
    fn render_lists_window_groups() {
        let n = NodeState {
            id: "w".into(),
            kind: "window".into(),
            groups: vec![group(&[("service.name", "cart")], Some(42.0), 42)],
        };
        let out = render(std::slice::from_ref(&n));
        assert!(out.contains("w (window) - 1 group(s)"));
        assert!(out.contains("service.name=cart"));
        assert!(out.contains("window=[0,5000000000)"));
        assert!(out.contains("value=42"));
        assert!(out.contains("samples=42"));
    }

    #[test]
    fn render_shows_join_inputs_and_sorts_pairs() {
        let mut g = group(&[], None, 0);
        g.inputs = [("b".to_string(), 2.0), ("a".to_string(), 1.0)]
            .into_iter()
            .collect();
        let n = NodeState {
            id: "j".into(),
            kind: "join".into(),
            groups: vec![g],
        };
        let out = render(std::slice::from_ref(&n));
        assert!(
            out.contains("inputs={a=1,b=2}"),
            "pairs sorted, no value: {out}"
        );
        assert!(
            !out.contains("value="),
            "a join bucket has no computed value"
        );
    }

    #[test]
    fn render_handles_no_nodes() {
        assert_eq!(render(&[]), "no stateful nodes\n");
    }

    #[test]
    fn output_ids_covers_sources_and_transforms_not_sinks() {
        let p: Pipeline = serde_norway::from_str(
            "sources: [{ type: generator, id: gen, interval: 1s }]\n\
             transforms: [{ type: window, id: w, input: gen, size: 5s, aggregate: { op: count } }]\n\
             sinks: [{ type: stdout, id: out, input: w }]\n",
        )
        .unwrap();
        // Sinks are terminal, so they produce no stream to provision.
        assert_eq!(output_ids(&p), vec!["gen".to_string(), "w".to_string()]);
    }

    #[test]
    fn pipeline_name_defaults_to_the_file_stem() {
        assert_eq!(pipeline_name(Path::new("examples/latency.yaml")), "latency");
        assert_eq!(pipeline_name(Path::new("noext")), "noext");
    }

    #[tokio::test]
    async fn run_pipeline_rejects_an_invalid_partition_config() {
        // A bad partition shape must fail fast, before any network connect.
        let p: Pipeline = serde_norway::from_str(
            "sources: [{ type: generator, id: g, interval: 1s }]\n\
             sinks: [{ type: stdout, id: o, input: g }]\n",
        )
        .unwrap();
        let nats = NatsOpts {
            url: Some("nats://127.0.0.1:4222".into()),
            name: None,
            part: headrace_core::backend::PartitionConfig {
                partitions: 2,
                workers: 4,
                index: 0,
            },
        };
        let err = run_pipeline(
            p,
            BackendKind::Nats,
            nats,
            Path::new("x.yaml"),
            Arc::new(headrace_core::NoopMetrics),
            headrace_core::RunOptions::default(),
        )
        .await
        .expect_err("an invalid partition config must be rejected");
        assert!(err.to_string().contains("workers"), "{err}");
    }
}
