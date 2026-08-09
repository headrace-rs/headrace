//! The gRPC `State` service (ADR-0014): answers snapshot queries by forwarding them to each
//! node's inspect channel and mapping the reply to the `headrace.v1` wire types.
//!
//! Read-only and unauthenticated - meant for a trusted admin network (localhost, a debug
//! sidecar), so it is opt-in behind `--inspect-addr` and binds its own port.

use super::{GroupSnapshot, NodeSnapshot, Registry};
use headrace_proto::v1::state_server::{State, StateServer};
use headrace_proto::v1::{GetRequest, GetResponse, GroupState, NodeState, WatchRequest};
use std::net::SocketAddr;
use std::pin::Pin;
use tokio::sync::{broadcast, oneshot};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// Serves [`Registry`] snapshots over the `State` service.
struct StateService {
    registry: Registry,
}

#[tonic::async_trait]
impl State for StateService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        // Empty request = every stateful node on this worker.
        let want = request.into_inner().node;
        let ids = if want.is_empty() {
            self.registry.ids()
        } else {
            want
        };
        let mut nodes = Vec::new();
        for id in ids {
            // A requested-but-unknown or already-exited node is simply omitted.
            if let Some(snap) = self.registry.query(&id).await {
                nodes.push(node_state(id, snap));
            }
        }
        Ok(Response::new(GetResponse { nodes }))
    }

    type WatchStream = Pin<Box<dyn Stream<Item = Result<NodeState, Status>> + Send>>;

    /// Stream each watched node's snapshot as its state changes. One forwarder task per node
    /// bridges the node's broadcast of change events into a single response stream; a node
    /// that falls behind the buffer resumes from the latest, and an unknown node is dropped.
    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let want = request.into_inner().node;
        let ids = if want.is_empty() {
            self.registry.ids()
        } else {
            want
        };
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<NodeState, Status>>(64);
        for id in ids {
            let Some(mut events) = self.registry.subscribe(&id) else {
                continue;
            };
            let tx = tx.clone();
            tokio::spawn(async move {
                loop {
                    match events.recv().await {
                        Ok(snap) => {
                            if tx.send(Ok(node_state(id.clone(), snap))).await.is_err() {
                                break; // the client hung up
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {} // resume from latest
                        Err(broadcast::error::RecvError::Closed) => break, // the node exited
                    }
                }
            });
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Map a node's snapshot onto the wire type, tagged with its registry id.
fn node_state(id: String, snap: NodeSnapshot) -> NodeState {
    NodeState {
        id,
        kind: snap.kind.to_string(),
        groups: snap.groups.into_iter().map(Into::into).collect(),
    }
}

/// A snapshot group maps directly onto its wire type. (The `id` for a `NodeState` comes from
/// the registry, not the snapshot, so that conversion stays inline in `get`.)
impl From<GroupSnapshot> for GroupState {
    fn from(g: GroupSnapshot) -> Self {
        GroupState {
            labels: g.labels.into_iter().collect(),
            window_start_nanos: g.start_nanos,
            window_end_nanos: g.end_nanos,
            value: g.value,
            inputs: g.inputs.into_iter().collect(),
            samples: g.samples,
        }
    }
}

/// Handle to the running `State` server. [`shutdown`](Server::shutdown) stops it gracefully;
/// dropping it without that is a backstop that aborts the task so the port never leaks.
pub struct Server {
    stop: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Stop accepting, let in-flight requests finish, and wait for the server task to end -
    /// the same best-effort drain the pipeline itself does on shutdown.
    pub async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(()); // server task may already be gone
        }
        let _ = (&mut self.task).await;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Backstop for a caller that never called `shutdown` (e.g. a panic unwinding past
        // it): don't leak the task. A graceful `shutdown` has already finished by here.
        self.task.abort();
    }
}

/// Spawn the `State` server on `addr`, serving `registry`. Reflection is registered from the
/// checked-in descriptor, so `grpcurl` works against a running pipeline. A bind or serve
/// error is logged, not fatal to the pipeline (inspection is a side surface).
pub fn spawn(registry: Registry, addr: SocketAddr) -> Server {
    let svc = StateServer::new(StateService { registry });
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(headrace_proto::FILE_DESCRIPTOR_SET)
        .build_v1()
        .expect("checked-in descriptor is valid");
    let (stop, stop_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tracing::info!(%addr, "state inspection server listening");
        let served = tonic::transport::Server::builder()
            .add_service(svc)
            .add_service(reflection)
            .serve_with_shutdown(addr, async {
                let _ = stop_rx.await;
            });
        if let Err(e) = served.await {
            tracing::error!(%addr, error = %e, "state inspection server stopped");
        }
    });
    Server {
        stop: Some(stop),
        task,
    }
}
