use crate::backend::Backend;
use crate::error::ValidationError;
use crate::metrics::{NodeKind, NodeMetrics, SharedMetrics};
use crate::{sink, source, transform};
use anyhow::{Result, anyhow};
use headrace_ir::{Pipeline, Source, Transform};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
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
        match o {
            Transform::Window {
                id,
                size,
                slide,
                allowed_lateness,
                idle_timeout,
                ..
            } => {
                let size = parse_duration(id, size)?;
                if let Some(lateness) = allowed_lateness {
                    parse_duration(id, lateness)?;
                }
                if let Some(timeout) = idle_timeout {
                    parse_duration(id, timeout)?;
                }
                if let Some(slide) = slide {
                    let slide = parse_duration(id, slide)?;
                    if slide.is_zero() {
                        return Err(ValidationError::InvalidWindow {
                            node: id.to_string(),
                            reason: "slide must be greater than zero".to_string(),
                        });
                    }
                    if slide > size {
                        return Err(ValidationError::InvalidWindow {
                            node: id.to_string(),
                            reason: "slide is longer than size; records would fall between windows"
                                .to_string(),
                        });
                    }
                }
            }
            Transform::Map { id, value, .. } => {
                crate::transform::expr::Expr::parse(value).map_err(|e| {
                    ValidationError::BadExpression {
                        node: id.to_string(),
                        reason: e.0,
                    }
                })?;
            }
            Transform::Join {
                id,
                value: Some(value),
                ..
            } => {
                crate::transform::expr::Expr::parse(value).map_err(|e| {
                    ValidationError::BadExpression {
                        node: id.to_string(),
                        reason: e.0,
                    }
                })?;
            }
            _ => {}
        }
    }

    // 3. Every input resolves, and each output has at most one consumer.
    let mut consumed = HashSet::new();
    for o in &p.transforms {
        for input in o.inputs() {
            check_edge(input, &outputs, &mut consumed)?;
        }
    }
    for s in &p.sinks {
        check_edge(s.input(), &outputs, &mut consumed)?;
    }

    // 4. Every transform is reachable from a source - rejects cycles and orphans.
    check_reachable(p)?;

    // 5. Join inputs are aligned windows; an align-only join feeds a transform.
    check_joins(p)?;
    Ok(())
}

/// Join inputs must be windows sharing a `group_by` and window size (so their
/// epoch-aligned bounds coincide), and an align-only join (no `value`) must feed a
/// transform, not a sink (its records carry no computed value).
fn check_joins(p: &Pipeline) -> Result<(), ValidationError> {
    let windows: HashMap<&str, (&[String], &str)> = p
        .transforms
        .iter()
        .filter_map(|o| match o {
            Transform::Window {
                id, size, group_by, ..
            } => Some((id.as_str(), (group_by.as_slice(), size.as_str()))),
            _ => None,
        })
        .collect();
    for o in &p.transforms {
        let Transform::Join { id, inputs, .. } = o else {
            continue;
        };
        let mut spec: Option<(&[String], &str)> = None;
        for input in inputs {
            let s = *windows
                .get(input.as_str())
                .ok_or_else(|| ValidationError::InvalidJoin {
                    node: id.clone(),
                    reason: format!("input `{input}` must be a window"),
                })?;
            match spec {
                None => spec = Some(s),
                Some(prev) if prev != s => {
                    return Err(ValidationError::InvalidJoin {
                        node: id.clone(),
                        reason: "inputs must share group_by and window size".to_string(),
                    });
                }
                _ => {}
            }
        }
    }
    for s in &p.sinks {
        if let Some(Transform::Join {
            id, value: None, ..
        }) = p.transforms.iter().find(|o| o.id() == s.input())
        {
            return Err(ValidationError::InvalidJoin {
                node: id.clone(),
                reason: "an align-only join (no `value`) must feed a transform, not a sink"
                    .to_string(),
            });
        }
    }
    Ok(())
}

fn node_ids(p: &Pipeline) -> impl Iterator<Item = &str> {
    p.sources
        .iter()
        .map(|s| s.id())
        .chain(p.transforms.iter().map(|o| o.id()))
        .chain(p.sinks.iter().map(|s| s.id()))
}

fn parse_duration(node: &str, value: &str) -> Result<Duration, ValidationError> {
    humantime::parse_duration(value).map_err(|source| ValidationError::BadDuration {
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
        for input in o.inputs() {
            consumers.entry(input).or_default().push(o.id());
        }
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

/// Optional side surfaces layered on the data pipeline.
#[derive(Default)]
pub struct RunOptions {
    /// Address for the state-inspection gRPC server (ADR-0014). `None` (the default) leaves
    /// it off.
    #[cfg(feature = "inspect")]
    pub inspect_addr: Option<std::net::SocketAddr>,
}

/// Wire the graph onto `backend` and run.
///
/// Runs until a shutdown signal (Ctrl-C / SIGTERM), a node failing or panicking
/// (fail fast, surfacing the error), or all nodes completing on their own. On a
/// signal it drains best-effort: sources stop, their dropped senders close the
/// graph so windows flush and sinks empty; a second signal forces an abort.
pub async fn run(
    p: Pipeline,
    mut backend: impl Backend,
    metrics: SharedMetrics,
    opts: RunOptions,
) -> Result<()> {
    #[cfg(not(feature = "inspect"))]
    let _ = opts; // no side surfaces compiled in
    validate(&p)?;
    let mut sources: JoinSet<Result<()>> = JoinSet::new();
    let mut work: JoinSet<Result<()>> = JoinSet::new();
    #[cfg(feature = "inspect")]
    let mut registry = crate::inspect::Registry::default();

    for s in &p.sources {
        let tx = backend.producer(s.id());
        let nm = NodeMetrics::bind(&metrics, s.id(), NodeKind::Source);
        let node = nm.clone();
        let src = s.clone();
        sources.spawn(async move {
            let r = source::run(src, tx, nm).await;
            record_error(&r, &node);
            r
        });
    }
    for o in &p.transforms {
        let rxs: Vec<_> = o.inputs().iter().map(|id| backend.consumer(id)).collect();
        let tx = backend.producer(o.id());
        let nm = NodeMetrics::bind(&metrics, o.id(), transform_kind(o));
        let node = nm.clone();
        let op = o.clone();
        // Wire this node for inspection only when the State server is on and it holds
        // inspectable state; otherwise the node's inspect arm stays dormant.
        #[cfg(feature = "inspect")]
        let inspect = if opts.inspect_addr.is_some() && is_inspectable(o) {
            Some(registry.register(o.id()))
        } else {
            None
        };
        #[cfg(not(feature = "inspect"))]
        let inspect: Option<crate::inspect::Inspector> = None;
        work.spawn(async move {
            let r = transform::run(op, rxs, tx, nm, inspect).await;
            record_error(&r, &node);
            r
        });
    }
    for s in &p.sinks {
        let rx = backend.consumer(s.input());
        let nm = NodeMetrics::bind(&metrics, s.id(), NodeKind::Sink);
        let node = nm.clone();
        let snk = s.clone();
        work.spawn(async move {
            let r = sink::run(snk, rx, nm).await;
            record_error(&r, &node);
            r
        });
    }
    // Release the backend's retained senders so channel-close propagates on drain.
    drop(backend);

    // The State server runs alongside the pipeline.
    #[cfg(feature = "inspect")]
    let inspect_server = start_inspect(opts, registry);

    tracing::info!(
        sources = p.sources.len(),
        transforms = p.transforms.len(),
        sinks = p.sinks.len(),
        "pipeline running; Ctrl-C/SIGTERM to stop"
    );

    let result = supervise_and_drain(&mut sources, &mut work).await;

    // Stop the inspection server the same way the pipeline stops: gracefully, once the work
    // has settled, so a final inspect during drain still succeeds.
    #[cfg(feature = "inspect")]
    if let Some(server) = inspect_server {
        server.shutdown().await;
    }

    result
}

/// Run until a shutdown signal, a node failure, or natural completion; on a signal, drain
/// best-effort (sources stop, their dropped senders close the graph so windows flush and
/// sinks empty; a second signal forces an abort).
async fn supervise_and_drain(
    sources: &mut JoinSet<Result<()>>,
    work: &mut JoinSet<Result<()>>,
) -> Result<()> {
    // Phase 1: run until a signal, a node failure, or natural completion.
    let signalled = tokio::select! {
        res = supervise(sources, work) => return res,
        _ = shutdown_signal() => true,
    };

    // Phase 2: best-effort drain.
    if signalled {
        tracing::info!("shutdown signal received; draining (signal again to force)");
        sources.abort_all();
        tokio::select! {
            res = drain(work) => return res,
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

/// Whether a transform holds state the `State` server can snapshot. Window today; join
/// snapshots are a follow-up (ADR-0014).
#[cfg(feature = "inspect")]
fn is_inspectable(o: &Transform) -> bool {
    matches!(o, Transform::Window { .. })
}

/// Start the state-inspection server if an address is configured and there is something to
/// inspect. Returns a guard that stops the server when dropped.
#[cfg(feature = "inspect")]
fn start_inspect(
    opts: RunOptions,
    registry: crate::inspect::Registry,
) -> Option<crate::inspect::server::Server> {
    match opts.inspect_addr {
        Some(_) if registry.is_empty() => {
            tracing::warn!("--inspect-addr set but the pipeline has no inspectable nodes");
            None
        }
        Some(addr) => Some(crate::inspect::server::spawn(registry, addr)),
        None => None,
    }
}

fn transform_kind(o: &Transform) -> NodeKind {
    match o {
        Transform::Filter { .. } => NodeKind::Filter,
        Transform::Window { .. } => NodeKind::Window,
        Transform::Map { .. } => NodeKind::Map,
        Transform::Join { .. } => NodeKind::Join,
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
pub(crate) async fn shutdown_signal() {
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
    fn rejects_bad_allowed_lateness_duration() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms:
              - { type: window, id: w, input: gen, size: 5s,
                  allowed_lateness: "3 fortnights", aggregate: { op: count } }
            sinks: [{ type: stdout, id: out, input: w }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::BadDuration { .. })
        ));
    }

    #[test]
    fn rejects_slide_longer_than_size() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms:
              - { type: window, id: w, input: gen, size: 5s, slide: 10s, aggregate: { op: count } }
            sinks: [{ type: stdout, id: out, input: w }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::InvalidWindow { .. })
        ));
    }

    #[test]
    fn accepts_a_join() {
        let p = pipeline(
            r#"
            sources:
              - { type: generator, id: s1, interval: 1s }
              - { type: generator, id: s2, interval: 1s }
            transforms:
              - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: count } }
              - { type: window, id: w2, input: s2, size: 5s, aggregate: { op: count } }
              - { type: join, id: j, inputs: [w1, w2], value: "w1 + w2" }
            sinks: [{ type: stdout, id: out, input: j }]
        "#,
        );
        assert!(validate(&p).is_ok());
    }

    #[test]
    fn rejects_join_with_dangling_input() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: s1, interval: 1s }]
            transforms:
              - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: count } }
              - { type: join, id: j, inputs: [w1, nope], value: "w1" }
            sinks: [{ type: stdout, id: out, input: j }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::UnknownInput(_))
        ));
    }

    #[test]
    fn rejects_join_over_a_non_window() {
        // A map output is not an aligned window, so it cannot be a join input.
        let p = pipeline(
            r#"
            sources:
              - { type: generator, id: s1, interval: 1s }
              - { type: generator, id: s2, interval: 1s }
            transforms:
              - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: count } }
              - { type: window, id: w2, input: s2, size: 5s, aggregate: { op: count } }
              - { type: map, id: m, input: w2, value: "value * 2" }
              - { type: join, id: j, inputs: [w1, m], value: "w1 + m" }
            sinks: [{ type: stdout, id: out, input: j }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::InvalidJoin { .. })
        ));
    }

    #[test]
    fn rejects_join_over_mismatched_windows() {
        let p = pipeline(
            r#"
            sources:
              - { type: generator, id: s1, interval: 1s }
              - { type: generator, id: s2, interval: 1s }
            transforms:
              - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: count } }
              - { type: window, id: w2, input: s2, size: 10s, aggregate: { op: count } }
              - { type: join, id: j, inputs: [w1, w2], value: "w1 + w2" }
            sinks: [{ type: stdout, id: out, input: j }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::InvalidJoin { .. })
        ));
    }

    #[test]
    fn rejects_align_only_join_into_a_sink() {
        // No `value`, so the join carries per-input values but no computed value; a sink
        // needs one, so this must fail.
        let p = pipeline(
            r#"
            sources:
              - { type: generator, id: s1, interval: 1s }
              - { type: generator, id: s2, interval: 1s }
            transforms:
              - { type: window, id: w1, input: s1, size: 5s, aggregate: { op: count } }
              - { type: window, id: w2, input: s2, size: 5s, aggregate: { op: count } }
              - { type: join, id: j, inputs: [w1, w2] }
            sinks: [{ type: stdout, id: out, input: j }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::InvalidJoin { .. })
        ));
    }

    #[test]
    fn rejects_bad_map_expression() {
        let p = pipeline(
            r#"
            sources: [{ type: generator, id: gen, interval: 1s }]
            transforms: [{ type: map, id: m, input: gen, value: "1 +" }]
            sinks: [{ type: stdout, id: out, input: m }]
        "#,
        );
        assert!(matches!(
            validate(&p),
            Err(ValidationError::BadExpression { .. })
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
        let err = run(
            p,
            InProcess::default(),
            metrics.clone(),
            RunOptions::default(),
        )
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
        fn record_late(&self, _: u64) {}
        fn window_flushed(&self, _: u64) {}
        fn node_error(&self) {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
