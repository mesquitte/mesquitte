use std::collections::{BTreeMap, BTreeSet};

use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use log::info;

use super::{app::App, store::Request, typ::RaftMetrics, Node, NodeId};

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

pub async fn add_learner(
    State(app): State<App>,
    Json(req): Json<(u64, String, String)>,
) -> impl IntoResponse {
    let node = Node {
        rpc_addr: req.1,
        api_addr: req.2,
    };
    let res = app.raft.add_learner(req.0, node, true).await;
    Json(res)
}

pub async fn change_membership(
    State(app): State<App>,
    Json(node_ids): Json<BTreeSet<NodeId>>,
) -> impl IntoResponse {
    let res = app.raft.change_membership(node_ids, false).await;
    Json(res)
}

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

pub async fn metrics(State(app): State<App>) -> Result<Json<RaftMetrics>, (StatusCode, String)> {
    let metrics = app.raft.metrics().borrow().clone();
    Ok(Json(metrics))
}
