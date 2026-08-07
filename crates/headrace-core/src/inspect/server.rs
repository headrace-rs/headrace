//! The gRPC `State` service (ADR-0014): answers snapshot queries by forwarding them to each
//! node's inspect channel and mapping the reply to the `headrace.v1` wire types.
//!
//! Read-only and unauthenticated - meant for a trusted admin network (localhost, a debug
//! sidecar), so it is opt-in behind `--inspect-addr` and binds its own port.

use super::{NodeSnapshot, Registry};
use headrace_proto::v1::state_server::{State, StateServer};
use headrace_proto::v1::{GetRequest, GetResponse, GroupState, NodeState};
use std::net::SocketAddr;
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
                nodes.push(to_proto(id, snap));
            }
        }
        Ok(Response::new(GetResponse { nodes }))
    }
}

/// Map a node's snapshot onto the wire type.
fn to_proto(id: String, snap: NodeSnapshot) -> NodeState {
    NodeState {
        id,
        kind: snap.kind.to_string(),
        groups: snap
            .groups
            .into_iter()
            .map(|g| GroupState {
                labels: g.labels.into_iter().collect(),
                window_start_nanos: g.start_nanos,
                window_end_nanos: g.end_nanos,
                value: g.value,
                inputs: g.inputs.into_iter().collect(),
                samples: g.samples,
            })
            .collect(),
    }
}

/// Aborts the served-server task when the pipeline run ends, so the port is released no
/// matter how `run` returns.
pub struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
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
    let task = tokio::spawn(async move {
        tracing::info!(%addr, "state inspection server listening");
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .add_service(reflection)
            .serve(addr)
            .await
        {
            tracing::error!(%addr, error = %e, "state inspection server stopped");
        }
    });
    Server(task)
}
