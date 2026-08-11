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

/// The partition/group key a record is published under, as raw bytes. A partitioned
/// backend hashes it to route every record for a key to the same worker - and thus the
/// same window state (DESIGN.md: group_key -> partition -> worker-local state). The
/// in-process backend ignores it. `None` means unkeyed.
pub type Key = Option<Vec<u8>>;

/// A named edge between two nodes. In-process today (mpsc); this is the boundary a NATS
/// JetStream impl (subject per node, partitioned by key, durable pull consumers) drops
/// into for the scaled deployment. Handles are cheap; a networked backend connects in
/// its constructor and does I/O per message in [`Producer::send`] / [`Consumer::recv`].
pub trait Backend: Send {
    /// Producer handle for node `id`'s output stream.
    fn producer(&mut self, id: &str) -> Box<dyn Producer>;
    /// Consumer handle for node `id`'s output stream. Single consumer per output.
    fn consumer(&mut self, id: &str) -> Box<dyn Consumer>;
}

/// Writes records to a node's output stream. `Sync`, because the node tasks call `send`
/// on a shared `&dyn Producer` held across `.await`.
#[cfg_attr(feature = "mocks", mockall::automock)]
#[async_trait]
pub trait Producer: Send + Sync {
    /// Publish `rec` under partition `key` (see [`Key`]). `Err` if the downstream is gone.
    async fn send(&self, key: Key, rec: Record) -> Result<()>;
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
    fn producer(&mut self, id: &str) -> Box<dyn Producer> {
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
    async fn send(&self, _key: Key, rec: Record) -> Result<()> {
        // In-process is single-worker: the partition key carries no routing meaning.
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
