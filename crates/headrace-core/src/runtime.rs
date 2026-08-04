use crate::backend::{Backend, Consumer, Producer};
use crate::error::ValidationError;
use crate::metrics::{NodeKind, NodeMetrics, SharedMetrics};
use crate::{sink, source, transform};
use anyhow::{Result, anyhow};
use headrace_ir::{Pipeline, Sink, Source, Transform};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use tokio::task::{JoinError, JoinSet};

/// Static checks: unique ids, durations parse, input refs resolve, each output has at
/// most one consumer, and every transform is reachable from a source (no cycles/orphans).
pub fn validate(p: &Pipeline) -> Result<(), ValidationError> {
    // 1. Unique ids across all nodes; collect the ids that can be referenced as an input.
    let mut all = HashSet::new();
    let mut outputs = HashSet::new();
    for id in node_ids(p) {
        if !all.insert(id) {
            return Err(ValidationError::DuplicateId(id.to_string()));
        }
    }
    for s in &p.sources {
        outputs.insert(s.id());
    }
    for o in &p.transforms {
        outputs.insert(o.id());
    }

    // 2. Durations parse here, not at runtime - a passing `validate` must be runnable.
    for s in &p.sources {
        if let Source::Generator { id, interval, .. } = s {
            parse_duration(id, interval)?;
        }
    }
    for o in &p.transforms {
        if let Transform::Window { id, size, .. } = o {
            parse_duration(id, size)?;
        }
    }

    // 3. Every input resolves, and each output has at most one consumer.
    let mut consumed = HashSet::new();
    for o in &p.transforms {
        check_edge(o.input(), &outputs, &mut consumed)?;
    }
    for s in &p.sinks {
        check_edge(s.input(), &outputs, &mut consumed)?;
    }

    // 4. Every transform is reachable from a source - rejects cycles and orphans.
    check_reachable(p)?;
    Ok(())
}

fn node_ids(p: &Pipeline) -> impl Iterator<Item = &str> {
    p.sources
        .iter()
        .map(|s| s.id())
        .chain(p.transforms.iter().map(|o| o.id()))
        .chain(p.sinks.iter().map(|s| s.id()))
}

fn parse_duration(node: &str, value: &str) -> Result<(), ValidationError> {
    humantime::parse_duration(value)
        .map(|_| ())
        .map_err(|source| ValidationError::BadDuration {
            node: node.to_string(),
            value: value.to_string(),
            source,
        })
}

fn check_edge<'a>(
    input: &'a str,
    outputs: &HashSet<&str>,
    consumed: &mut HashSet<&'a str>,
) -> Result<(), ValidationError> {
    if !outputs.contains(input) {
        return Err(ValidationError::UnknownInput(input.to_string()));
    }
    if !consumed.insert(input) {
        return Err(ValidationError::MultipleConsumers(input.to_string()));
    }
    Ok(())
}

/// BFS forward from the sources over `input` edges. Any transform not reached is either
/// orphaned or part of a cycle (two transforms feeding each other pass every other check).
fn check_reachable(p: &Pipeline) -> Result<(), ValidationError> {
    let mut consumers: HashMap<&str, Vec<&str>> = HashMap::new();
    for o in &p.transforms {
        consumers.entry(o.input()).or_default().push(o.id());
    }
    let mut seen: HashSet<&str> = p.sources.iter().map(|s| s.id()).collect();
    let mut stack: Vec<&str> = seen.iter().copied().collect();
    while let Some(n) = stack.pop() {
        for &c in consumers.get(n).into_iter().flatten() {
            if seen.insert(c) {
                stack.push(c);
            }
        }
    }
    for o in &p.transforms {
        if !seen.contains(o.id()) {
            return Err(ValidationError::Unreachable(o.id().to_string()));
        }
    }
    Ok(())
}

/// A node's task: runs one source, transform, or sink to completion.
pub type NodeFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Builds tasks for source and sink kinds that `headrace-core` does not implement itself
/// (OTLP lives in `headrace-otlp`). The runtime handles its built-in generator/stdin/stdout
/// directly and delegates every other variant here.
pub trait ExternalNodes: Send + Sync {
    fn source(&self, src: Source, tx: Box<dyn Producer>, nm: NodeMetrics) -> Result<NodeFuture>;
    fn sink(&self, sink: Sink, rx: Box<dyn Consumer>, nm: NodeMetrics) -> Result<NodeFuture>;
}

/// The default: no external kinds, so an unrecognized source or sink is a config error.
pub struct NoExternalNodes;

impl ExternalNodes for NoExternalNodes {
    fn source(&self, src: Source, _tx: Box<dyn Producer>, _nm: NodeMetrics) -> Result<NodeFuture> {
        Err(anyhow!("unsupported source `{}`", src.id()))
    }
    fn sink(&self, sink: Sink, _rx: Box<dyn Consumer>, _nm: NodeMetrics) -> Result<NodeFuture> {
        Err(anyhow!("unsupported sink `{}`", sink.id()))
    }
}

fn build_source(
    src: Source,
    tx: Box<dyn Producer>,
    nm: NodeMetrics,
    external: &dyn ExternalNodes,
) -> Result<NodeFuture> {
    match &src {
        Source::Generator { .. } | Source::Stdin { .. } => Ok(Box::pin(source::run(src, tx, nm))),
        _ => external.source(src, tx, nm),
    }
}

fn build_sink(
    sink: Sink,
    rx: Box<dyn Consumer>,
    nm: NodeMetrics,
    external: &dyn ExternalNodes,
) -> Result<NodeFuture> {
    match &sink {
        Sink::Stdout { .. } => Ok(Box::pin(sink::run(sink, rx, nm))),
        _ => external.sink(sink, rx, nm),
    }
}

/// Wire the graph onto `backend` and run, using only the built-in node kinds.
pub async fn run(p: Pipeline, backend: impl Backend, metrics: SharedMetrics) -> Result<()> {
    run_with(p, backend, metrics, &NoExternalNodes).await
}

/// Like [`run`], but source and sink kinds beyond the built-ins are built by `external`
/// (for example OTLP, from `headrace-otlp`).
///
/// Runs until a shutdown signal (Ctrl-C / SIGTERM), a node failing or panicking
/// (fail fast, surfacing the error), or all nodes completing on their own. On a
/// signal it drains best-effort: sources stop, their dropped senders close the
/// graph so windows flush and sinks empty; a second signal forces an abort.
pub async fn run_with(
    p: Pipeline,
    mut backend: impl Backend,
    metrics: SharedMetrics,
    external: &dyn ExternalNodes,
) -> Result<()> {
    validate(&p)?;
    let mut sources: JoinSet<Result<()>> = JoinSet::new();
    let mut work: JoinSet<Result<()>> = JoinSet::new();

    for s in &p.sources {
        let tx = backend.producer(s.id());
        let nm = NodeMetrics::bind(&metrics, s.id(), NodeKind::Source);
        let node = nm.clone();
        let fut = build_source(s.clone(), tx, nm, external)?;
        sources.spawn(async move {
            let r = fut.await;
            record_error(&r, &node);
            r
        });
    }
    for o in &p.transforms {
        let rx = backend.consumer(o.input());
        let tx = backend.producer(o.id());
        let nm = NodeMetrics::bind(&metrics, o.id(), transform_kind(o));
        let node = nm.clone();
        let op = o.clone();
        work.spawn(async move {
            let r = transform::run(op, rx, tx, nm).await;
            record_error(&r, &node);
            r
        });
    }
    for s in &p.sinks {
        let rx = backend.consumer(s.input());
        let nm = NodeMetrics::bind(&metrics, s.id(), NodeKind::Sink);
        let node = nm.clone();
        let fut = build_sink(s.clone(), rx, nm, external)?;
        work.spawn(async move {
            let r = fut.await;
            record_error(&r, &node);
            r
        });
    }
    // Release the backend's retained senders so channel-close propagates on drain.
    drop(backend);

    tracing::info!(
        sources = p.sources.len(),
        transforms = p.transforms.len(),
        sinks = p.sinks.len(),
        "pipeline running; Ctrl-C/SIGTERM to stop"
    );

    // Phase 1: run until a signal, a node failure, or natural completion.
    let signalled = tokio::select! {
        res = supervise(&mut sources, &mut work) => return res,
        _ = shutdown_signal() => true,
    };

    // Phase 2: best-effort drain.
    if signalled {
        tracing::info!("shutdown signal received; draining (signal again to force)");
        sources.abort_all();
        tokio::select! {
            res = drain(&mut work) => return res,
            _ = shutdown_signal() => {
                tracing::warn!("second signal; aborting in-flight work");
                work.abort_all();
            }
        }
    }
    Ok(())
}

/// Map a joined task outcome to a pipeline result. A node returning `Err` or
/// panicking fails the pipeline; a cancelled node (from `abort`) is expected.
fn propagate(joined: Result<Result<()>, JoinError>) -> Result<()> {
    match joined {
        Ok(inner) => inner,
        Err(e) if e.is_panic() => Err(anyhow!("node panicked: {e}")),
        Err(_) => Ok(()), // cancelled during shutdown
    }
}

/// Meter a node-error when its task returned `Err` (recorded here, where the id is known).
fn record_error(result: &Result<()>, nm: &NodeMetrics) {
    if result.is_err() {
        nm.error();
    }
}

fn transform_kind(o: &Transform) -> NodeKind {
    match o {
        Transform::Filter { .. } => NodeKind::Filter,
        Transform::Window { .. } => NodeKind::Window,
        // Forward-compat: an unknown transform fails in `transform::run`; kind is cosmetic.
        _ => NodeKind::Filter,
    }
}

/// Await both task sets until one fails or all finish.
async fn supervise(
    sources: &mut JoinSet<Result<()>>,
    work: &mut JoinSet<Result<()>>,
) -> Result<()> {
    loop {
        tokio::select! {
            Some(joined) = sources.join_next() => propagate(joined)?,
            Some(joined) = work.join_next() => propagate(joined)?,
            else => return Ok(()),
        }
    }
}

/// Await the remaining tasks, surfacing the first failure.
async fn drain(work: &mut JoinSet<Result<()>>) -> Result<()> {
    while let Some(joined) = work.join_next().await {
        propagate(joined)?;
    }
    Ok(())
}

/// Completes on Ctrl-C or, on Unix, SIGTERM (Kubernetes sends SIGTERM on pod stop).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::InProcess;
    use crate::metrics::{Metrics, NodeRecorder};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn accepts_a_valid_pipeline() {
        assert!(validate(&pipeline(GOOD)).is_ok());
    }

    #[test]
    fn rejects_duplicate_id() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: dup, interval: 1s }]
            transforms: [{ type: filter, id: dup, input: dup, key: k }]
            sinks: [{ type: stdout, id: out, input: dup }]
        "#,
        );
        assert!(matches!(validate(&p), Err(ValidationError::DuplicateId(_))));
    }

    #[test]
    fn rejects_dangling_input() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            sinks: [{ type: stdout, id: out, input: nope }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::UnknownInput(_))
        ));
    }

    #[test]
    fn rejects_fanout() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms: [{ type: filter, id: f, input: gen, key: k }]
            sinks:
              - { type: stdout, id: a, input: gen }
              - { type: stdout, id: b, input: gen }
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::MultipleConsumers(_))
        ));
    }

    #[test]
    fn rejects_bad_duration_before_running() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms: [{ type: window, id: w, input: gen, size: "5 furlongs", aggregate: { op: count } }]
            sinks: [{ type: stdout, id: out, input: w }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::BadDuration { .. })
        ));
    }

    #[test]
    fn rejects_cycle() {
        // a -> b -> a, with no source feeding either: passes id/edge/fanout checks,
        // but is unreachable from any source.
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms:
              - { type: filter, id: a, input: b, key: k }
              - { type: filter, id: b, input: a, key: k }
            sinks: [{ type: stdout, id: out, input: gen }]
        "#,
        );
        assert!(matches!(validate(&p), Err(ValidationError::Unreachable(_))));
    }

    #[test]
    fn propagate_maps_outcomes() {
        assert!(propagate(Ok(Ok(()))).is_ok());
        assert!(propagate(Ok(Err(anyhow!("boom")))).is_err());
    }

    #[tokio::test]
    async fn a_failing_node_stops_the_run_and_meters_the_error() {
        // `on_missing: error` on an absent field fails the window on the first record;
        // supervision must surface it and return without waiting for a signal.
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: g, interval: 5ms }]
            transforms:
              - { type: window, id: w, input: g, size: 1h,
                  aggregate: { op: avg, field: nope, on_missing: error } }
            sinks: [{ type: stdout, id: o, input: w }]
        "#,
        );
        let metrics = Arc::new(ErrCounts::default());
        let err = run(p, InProcess::default(), metrics.clone())
            .await
            .expect_err("missing field should fail the run");
        assert!(err.to_string().contains("missing numeric field"));
        assert!(
            metrics.errors.load(Ordering::Relaxed) >= 1,
            "the failing node's error must be metered"
        );
    }

    fn pipeline(yaml: &str) -> Pipeline {
        serde_yaml::from_str(yaml).expect("valid yaml")
    }

    const GOOD: &str = r#"
        sources:   [{ type: generator, id: gen, interval: 200ms }]
        transforms: [{ type: window, id: w, input: gen, size: 5s, aggregate: { op: count } }]
        sinks:     [{ type: stdout, id: out, input: w }]
    "#;

    #[derive(Default)]
    struct ErrCounts {
        errors: Arc<AtomicU64>,
    }
    impl Metrics for ErrCounts {
        fn node(&self, _: &str, _: NodeKind) -> Arc<dyn NodeRecorder> {
            Arc::new(ErrRecorder {
                errors: self.errors.clone(),
            })
        }
    }
    struct ErrRecorder {
        errors: Arc<AtomicU64>,
    }
    impl NodeRecorder for ErrRecorder {
        fn record_out(&self) {}
        fn record_dropped(&self, _: u64) {}
        fn window_flushed(&self, _: u64) {}
        fn node_error(&self) {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
