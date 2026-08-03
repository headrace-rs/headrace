//! headrace's own telemetry - a boundary so the core records metrics without depending on any
//! particular SDK. The binary supplies an OTel-backed [`Metrics`]; tests supply fakes;
//! the default is [`NoopMetrics`].
//!
//! [`Metrics::node`] hands out a per-node [`NodeRecorder`] once at wiring time, so the
//! implementation can cache its attribute set and instrument handles - the per-record
//! calls then allocate nothing. The recorders are the one piece of state shared across
//! node tasks, handed out as cheap `Arc` clones.

use std::sync::Arc;

/// The kind of node a metric is attributed to - a low-cardinality label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Source,
    Filter,
    Window,
    Sink,
}

impl NodeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            NodeKind::Source => "source",
            NodeKind::Filter => "filter",
            NodeKind::Window => "window",
            NodeKind::Sink => "sink",
        }
    }
}

/// Binds per-node recorders. `Send + Sync`; one instance is shared across all nodes.
pub trait Metrics: Send + Sync {
    /// Bind a recorder to one node. Called once per node at wiring time - the place to
    /// pre-compute attributes so the hot path stays allocation-free.
    fn node(&self, node: &str, kind: NodeKind) -> Arc<dyn NodeRecorder>;
}

/// Records one node's telemetry. Its attributes are fixed at construction, so these
/// calls - invoked once per record on the hot path - allocate nothing.
pub trait NodeRecorder: Send + Sync {
    /// A record was emitted/forwarded.
    fn record_out(&self);
    /// `n` records were dropped (filtered out, or `on_missing = skip`).
    fn record_dropped(&self, n: u64);
    /// A window flushed, emitting `groups` aggregates.
    fn window_flushed(&self, groups: u64);
    /// The node's task terminated with an error.
    fn node_error(&self);
}

/// The default: records nothing.
pub struct NoopMetrics;

impl Metrics for NoopMetrics {
    fn node(&self, _: &str, _: NodeKind) -> Arc<dyn NodeRecorder> {
        Arc::new(NoopRecorder)
    }
}

struct NoopRecorder;

impl NodeRecorder for NoopRecorder {
    fn record_out(&self) {}
    fn record_dropped(&self, _: u64) {}
    fn window_flushed(&self, _: u64) {}
    fn node_error(&self) {}
}

/// A [`Metrics`] shared across node tasks.
pub type SharedMetrics = Arc<dyn Metrics>;

/// One node's recorder, obtained via [`NodeMetrics::bind`]. Cheap to clone.
#[derive(Clone)]
pub struct NodeMetrics(Arc<dyn NodeRecorder>);

impl NodeMetrics {
    /// Bind a recorder for `node` from the shared [`Metrics`].
    pub fn bind(metrics: &SharedMetrics, node: &str, kind: NodeKind) -> Self {
        Self(metrics.node(node, kind))
    }

    pub fn out(&self) {
        self.0.record_out();
    }

    pub fn dropped(&self, n: u64) {
        self.0.record_dropped(n);
    }

    pub fn window_flushed(&self, groups: u64) {
        self.0.window_flushed(groups);
    }

    pub fn error(&self) {
        self.0.node_error();
    }
}
