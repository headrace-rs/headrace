use crate::record::Record;
use anyhow::{Result, anyhow};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::mpsc;

/// NATS JetStream backend for the scaled deployment (ADR-0015), behind the `nats` feature.
#[cfg(feature = "nats")]
mod nats;
#[cfg(feature = "nats")]
pub use nats::Nats;

/// How a producer derives the partition key for each record: the ordered `group_by`
/// fields of the stateful transform this edge feeds, or `Unkeyed` when it feeds a
/// stateless node (partitioning is then irrelevant to correctness). The runtime computes
/// one per edge from the pipeline graph; a partitioned backend hashes the extracted key to
/// route every record for a key to the same worker - and thus the same window state
/// (DESIGN.md: group_key -> partition -> worker-local state). The in-process backend
/// ignores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeySpec {
    /// Key each record by these `group_by` fields, in order.
    Keyed(Vec<String>),
    /// No routing key; the consumer holds no keyed state.
    Unkeyed,
}

/// A named edge between two nodes. In-process today (mpsc); this is the boundary a NATS
/// JetStream impl (subject per node, partitioned by key, durable pull consumers) drops
/// into for the scaled deployment. Handles are cheap; a networked backend connects in
/// its constructor and does I/O per message in [`Producer::send`] / [`Consumer::recv`].
pub trait Backend: Send {
    /// Producer handle for node `id`'s output stream, keyed per `key` (see [`KeySpec`]).
    fn producer(&mut self, id: &str, key: &KeySpec) -> Box<dyn Producer>;
    /// Consumer handle for node `id`'s output stream. Single consumer per output.
    fn consumer(&mut self, id: &str) -> Box<dyn Consumer>;
}

/// Writes records to a node's output stream, partitioning by the [`KeySpec`] the producer
/// was built with. `Sync`, because the node tasks call `send` on a shared `&dyn Producer`
/// held across `.await`.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait Producer: Send + Sync {
    /// Publish `rec`, routed by the producer's key. `Err` if the downstream is gone.
    async fn send(&self, rec: Record) -> Result<()>;
}

/// Reads records from a node's output stream.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait Consumer: Send {
    /// The next record, or `None` when the upstream is closed and drained.
    async fn recv(&mut self) -> Option<Record>;
    // Acks arrive with the durable NATS backend (v0.2).
}

/// In-memory backend: one bounded mpsc channel per node. No external dependencies -
/// the monolithic (dev/edge) deployment.
pub struct InProcess {
    chans: HashMap<String, (mpsc::Sender<Record>, Option<mpsc::Receiver<Record>>)>,
    cap: usize,
}

impl InProcess {
    pub fn new(cap: usize) -> Self {
        Self {
            chans: HashMap::new(),
            cap,
        }
    }

    fn ensure(&mut self, id: &str) {
        if !self.chans.contains_key(id) {
            let (tx, rx) = mpsc::channel(self.cap);
            self.chans.insert(id.to_string(), (tx, Some(rx)));
        }
    }
}

/// Default channel capacity for [`InProcess`].
pub const DEFAULT_CAP: usize = 1024;

impl Default for InProcess {
    fn default() -> Self {
        Self::new(DEFAULT_CAP)
    }
}

impl Backend for InProcess {
    fn producer(&mut self, id: &str, _key: &KeySpec) -> Box<dyn Producer> {
        // Single-worker: the partition key carries no routing meaning.
        self.ensure(id);
        Box::new(ChannelProducer(self.chans[id].0.clone()))
    }

    fn consumer(&mut self, id: &str) -> Box<dyn Consumer> {
        self.ensure(id);
        // `ensure` just inserted this entry, so the lookup cannot miss. A second consumer of
        // one output is a wiring bug that `validate` (MultipleConsumers) rejects before we
        // reach here, so the `take` failing is unreachable in a validated pipeline.
        let rx = self
            .chans
            .get_mut(id)
            .expect("channel just ensured")
            .1
            .take()
            .unwrap_or_else(|| panic!("output `{id}` already has a consumer"));
        Box::new(ChannelConsumer(rx))
    }
}

struct ChannelProducer(mpsc::Sender<Record>);

#[async_trait]
impl Producer for ChannelProducer {
    async fn send(&self, rec: Record) -> Result<()> {
        self.0
            .send(rec)
            .await
            .map_err(|_| anyhow!("downstream closed"))
    }
}

struct ChannelConsumer(mpsc::Receiver<Record>);

#[async_trait]
impl Consumer for ChannelConsumer {
    async fn recv(&mut self) -> Option<Record> {
        self.0.recv().await
    }
}
