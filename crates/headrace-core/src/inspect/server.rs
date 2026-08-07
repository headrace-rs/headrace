//! The gRPC `State` service (ADR-0014): answers snapshot queries by forwarding them to each
//! node's inspect channel and mapping the reply to the `headrace.v1` wire types.
//!
//! Read-only and unauthenticated - meant for a trusted admin network (localhost, a debug
//! sidecar), so it is opt-in behind `--inspect-addr` and binds its own port.

use super::{GroupSnapshot, Registry};
use headrace_proto::v1::state_server::{State, StateServer};
use headrace_proto::v1::{GetRequest, GetResponse, GroupState, NodeState};
use std::net::SocketAddr;
use tokio::sync::oneshot;
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
                nodes.push(NodeState {
                    id,
                    kind: snap.kind.to_string(),
                    groups: snap.groups.into_iter().map(Into::into).collect(),
                });
            }
        }
        Ok(Response::new(GetResponse { nodes }))
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
