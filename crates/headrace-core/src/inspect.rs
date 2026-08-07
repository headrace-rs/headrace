//! State inspection plumbing: point-in-time snapshots pulled from a stateful node's own
//! task loop (ADR-0014).
//!
//! A snapshot is produced *by the node's own `select!` loop* in response to a query, so it
//! is consistent with in-flight processing - no shared lock, no torn read. These types are
//! always compiled (they are cheap and keep the node loops free of `cfg`); the gRPC surface
//! that consumes them lives behind the `inspect` feature.

use crate::record::Attrs;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

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

/// The node end of the inspect channel - queries arrive here (one `select!` arm reads it).
pub type Inspector = mpsc::Receiver<Query>;

/// The registry end - send a query to ask a node for its current snapshot.
pub type Handle = mpsc::Sender<Query>;

/// Await the next inspect query, or - when inspection is off (`None`) - never resolve, so
/// the node's `select!` arm stays dormant. Mirrors `window::maybe_sleep`. Returns `None`
/// when the channel has closed (every [`Handle`] dropped), so the caller can stop polling.
pub(crate) async fn recv_query(inspect: &mut Option<Inspector>) -> Option<Query> {
    match inspect {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Stringify a group's carried attributes into snapshot labels.
pub(crate) fn labels_of(attrs: &Attrs) -> BTreeMap<String, String> {
    attrs
        .iter()
        .map(|(k, v)| (k.clone(), v.to_string()))
        .collect()
}
