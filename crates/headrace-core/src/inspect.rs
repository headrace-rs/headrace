//! State inspection plumbing: point-in-time snapshots pulled from a stateful node's own
//! task loop (ADR-0014).
//!
//! A snapshot is produced *by the node's own `select!` loop* in response to a query, so it
//! is consistent with in-flight processing - no shared lock, no torn read. These types are
//! always compiled (they are cheap and keep the node loops free of `cfg`); the gRPC surface
//! that consumes them lives behind the `inspect` feature.

use crate::record::Attrs;
use std::collections::BTreeMap;
use tokio::sync::{broadcast, mpsc, oneshot};

/// One open window (or join bucket) for one group key, at snapshot time.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupSnapshot {
    /// `group_by` dimension -> stringified value.
    pub labels: BTreeMap<String, String>,
    pub start_nanos: u64,
    pub end_nanos: u64,
    /// Window: the group's current running aggregate. `None` for a join bucket, or for an
    /// empty min/max/avg group that has no value yet.
    pub value: Option<f64>,
    /// Join: per-input values filled so far, keyed by input id. Empty for a window.
    pub inputs: BTreeMap<String, f64>,
    /// Window: records folded into this group so far.
    pub samples: u64,
}

/// A stateful node's whole open state at snapshot time.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSnapshot {
    /// `"window"` or `"join"`.
    pub kind: &'static str,
    pub groups: Vec<GroupSnapshot>,
}

/// A pending inspect query: the node answers by sending its snapshot back on the channel.
pub type Query = oneshot::Sender<NodeSnapshot>;

/// The node end of the query channel - queries arrive here (one `select!` arm reads it).
pub type Inspector = mpsc::Receiver<Query>;

/// The registry end - send a query to ask a node for its current snapshot.
pub type Handle = mpsc::Sender<Query>;

/// How many change events a node buffers for `Watch`. A subscriber that falls behind skips
/// to the latest rather than blocking the node.
const EVENT_CAP: usize = 16;

/// Capacity of a node's query channel. Queries are rare and answered immediately from the
/// node's loop, so a small buffer is plenty.
const QUERY_CAP: usize = 8;

/// A stateful node's end of inspection: pull queries (`Get`) and push change events
/// (`Watch`). A `None` on a node means inspection is off, so both stay dormant.
pub struct Inspect {
    queries: Inspector,
    events: broadcast::Sender<NodeSnapshot>,
}

impl Inspect {
    /// A standalone inspection channel: the node end, the query [`Handle`], and a `Watch`
    /// event subscription. The registry keeps the handle and an events sender; a test can use
    /// the returned receiver to watch directly.
    pub fn channel() -> (Inspect, Handle, broadcast::Receiver<NodeSnapshot>) {
        let (query, queries) = mpsc::channel(QUERY_CAP);
        let (events, subscription) = broadcast::channel(EVENT_CAP);
        (Inspect { queries, events }, query, subscription)
    }
}

/// Await the next inspect query, or - when inspection is off (`None`) - never resolve, so
/// the node's `select!` arm stays dormant. Returns `None` when the query channel has closed
/// (every [`Handle`] dropped), so the caller can stop polling.
pub(crate) async fn recv_query(inspect: &mut Option<Inspect>) -> Option<Query> {
    match inspect {
        Some(i) => i.queries.recv().await,
        None => std::future::pending().await,
    }
}

/// Push a fresh snapshot to any `Watch` subscribers. The snapshot is built only when someone
/// is watching, so an unwatched node pays nothing.
pub(crate) fn publish(inspect: Option<&Inspect>, snapshot: impl FnOnce() -> NodeSnapshot) {
    if let Some(i) = inspect
        && i.events.receiver_count() > 0
    {
        let _ = i.events.send(snapshot());
    }
}

/// Smallest gap between `Watch` snapshots from one node. A node under `Watch` would
/// otherwise rebuild its entire open state on every record; coalescing to this interval
/// keeps the cost bounded while still tracking state closely enough for an operator.
const SNAPSHOT_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Like [`publish`], but at most once per [`SNAPSHOT_MIN_INTERVAL`]. `last` holds the node's
/// previous publish time (`None` until the first), updated in place; the first call always
/// publishes. Building the (potentially large) snapshot is skipped when throttled or unwatched.
pub(crate) fn publish_throttled(
    inspect: Option<&Inspect>,
    last: &mut Option<tokio::time::Instant>,
    snapshot: impl FnOnce() -> NodeSnapshot,
) {
    let now = tokio::time::Instant::now();
    if last.is_none_or(|t| now.duration_since(t) >= SNAPSHOT_MIN_INTERVAL) {
        *last = Some(now);
        publish(inspect, snapshot);
    }
}

/// Stringify a group's carried attributes into snapshot labels.
pub(crate) fn labels_of(attrs: &Attrs) -> BTreeMap<String, String> {
    attrs
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect()
}

/// The `State` gRPC service that serves these snapshots (ADR-0014).
#[cfg(feature = "inspect")]
pub mod server;

/// A node's inspection handles, kept by the registry: the query channel and the events
/// sender that `Watch` subscribes to.
#[cfg(feature = "inspect")]
struct Node {
    query: Handle,
    events: broadcast::Sender<NodeSnapshot>,
}

/// Maps each stateful node's id to its inspection handles. The runtime builds one as it
/// spawns nodes ([`Registry::register`]); the `State` server queries and subscribes through it.
#[cfg(feature = "inspect")]
#[derive(Default)]
pub struct Registry {
    nodes: std::collections::HashMap<String, Node>,
}

#[cfg(feature = "inspect")]
impl Registry {
    /// Wire node `id` for inspection: keep its query [`Handle`] and events sender, and hand
    /// back the [`Inspect`] end its loop drives.
    pub fn register(&mut self, id: &str) -> Inspect {
        let (query, queries) = mpsc::channel(QUERY_CAP);
        let (events, _) = broadcast::channel(EVENT_CAP);
        self.nodes.insert(
            id.to_string(),
            Node {
                query,
                events: events.clone(),
            },
        );
        Inspect { queries, events }
    }

    /// Whether any node is registered.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The registered node ids, sorted for deterministic responses.
    pub(crate) fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.nodes.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Ask node `id` for a current snapshot. `None` if it is unregistered or has exited
    /// (its loop dropped the [`Handle`]).
    pub(crate) async fn query(&self, id: &str) -> Option<NodeSnapshot> {
        let node = self.nodes.get(id)?;
        let (reply, rx) = oneshot::channel();
        node.query.send(reply).await.ok()?;
        rx.await.ok()
    }

    /// Subscribe to node `id`'s change events for `Watch`. `None` if it is unregistered.
    pub(crate) fn subscribe(&self, id: &str) -> Option<broadcast::Receiver<NodeSnapshot>> {
        Some(self.nodes.get(id)?.events.subscribe())
    }
}
