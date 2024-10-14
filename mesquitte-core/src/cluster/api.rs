use std::collections::{BTreeMap, BTreeSet};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use log::info;
use openraft::RaftMetrics;
use serde::Deserialize;

use super::{app::App, store::Request, Node, NodeId, TypeConfig};

pub async fn write(State(app): State<App>, Json(req): Json<Request>) -> impl IntoResponse {
    let res = app.raft.client_write(req).await;
    info!("appid: {}", app.id);
    Json(res)
}

pub async fn read(
    State(app): State<App>,
    Json(req): Json<String>,
) -> Result<Json<String>, (StatusCode, String)> {
    let state_machine = app.state_machine_store.sm.read();
    let value = state_machine.data.get(&req).cloned();
    Ok(Json(value.unwrap_or_default()))
}

// Management API

#[derive(Deserialize)]
pub struct AddLearnerRequest {
    node_id: u64,
    rpc_addr: String,
    api_addr: String,
}

/// Add a node as **Learner**.
pub async fn add_learner(
    State(app): State<App>,
    Json(req): Json<AddLearnerRequest>,
) -> impl IntoResponse {
    let node = Node {
        rpc_addr: req.rpc_addr,
        api_addr: req.api_addr,
    };
    let res = app.raft.add_learner(req.node_id, node, true).await;
    Json(res)
}

/// Changes specified learners to members, or remove members.
pub async fn change_membership(
    State(app): State<App>,
    Json(node_ids): Json<BTreeSet<NodeId>>,
) -> impl IntoResponse {
    let res = app.raft.change_membership(node_ids, false).await;
    Json(res)
}

/// Initialize a single-node cluster.
pub async fn init(State(app): State<App>) -> impl IntoResponse {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        Node {
            rpc_addr: app.rpc_addr.to_string(),
            api_addr: app.api_addr.to_string(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    Json(res)
}

/// Get the latest metrics of the cluster
pub async fn metrics(
    State(app): State<App>,
) -> Result<Json<RaftMetrics<TypeConfig>>, (StatusCode, String)> {
    let metrics = app.raft.metrics().borrow().clone();
    // let res: Result<RaftMetrics<TypeConfig>, Infallible> = Ok(metrics);
    Ok(Json(metrics))
}
