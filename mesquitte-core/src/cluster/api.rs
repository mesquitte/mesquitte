use std::collections::{BTreeMap, BTreeSet};

use axum::extract::State;
use openraft::{error::Infallible, RaftMetrics};

use super::{app::App, decode, encode, Node, NodeId, TypeConfig};

pub async fn write(State(app): State<App>, req: String) -> String {
    let res = app.raft.client_write(decode(&req)).await;
    encode(res)
}

pub async fn read(State(app): State<App>, req: String) -> String {
    String::default()
}

// Management API

/// Add a node as **Learner**.
pub async fn add_learner(State(app): State<App>, req: String) -> String {
    let node_id: NodeId = decode(&req);
    let node = Node {
        rpc_addr: "".to_string(),
        api_addr: "".to_string(),
    };
    let res = app.raft.add_learner(node_id, node, true).await;
    encode(res)
}

/// Changes specified learners to members, or remove members.
pub async fn change_membership(State(app): State<App>, req: String) -> String {
    let node_ids: BTreeSet<NodeId> = decode(&req);
    let res = app.raft.change_membership(node_ids, false).await;
    encode(res)
}

/// Initialize a single-node cluster.
pub async fn init(State(app): State<App>) -> String {
    let mut nodes = BTreeMap::new();
    nodes.insert(
        app.id,
        Node {
            rpc_addr: "".to_string(),
            api_addr: "".to_string(),
        },
    );
    let res = app.raft.initialize(nodes).await;
    encode(res)
}

/// Get the latest metrics of the cluster
pub async fn metrics(State(app): State<App>) -> String {
    let metrics = app.raft.metrics().borrow().clone();
    let res: Result<RaftMetrics<TypeConfig>, Infallible> = Ok(metrics);
    encode(res)
}
