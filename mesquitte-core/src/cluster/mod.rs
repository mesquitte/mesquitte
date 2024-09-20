use std::sync::Arc;

use app::App;
use openraft::{Config, Raft};
use router::Router;
use store::{Request, Response, StateMachine};

pub mod api;
pub mod app;
pub mod network;
pub mod router;
pub mod store;

pub type NodeId = u64;
pub type LogStore = store::RocksLogStore;
pub type StateMachineStore = store::RocksStateMachine;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        SnapshotData = StateMachine,
);

pub mod typ {

    use super::NodeId;
    use super::TypeConfig;

    pub type Raft = openraft::Raft<TypeConfig>;

    pub type Vote = openraft::Vote<NodeId>;
    pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
    pub type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;
    pub type Snapshot = openraft::Snapshot<TypeConfig>;

    pub type Infallible = openraft::error::Infallible;
    pub type Fatal = openraft::error::Fatal<TypeConfig>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
    pub type RPCError = openraft::error::RPCError<TypeConfig>;
    pub type StreamingError = openraft::error::StreamingError<TypeConfig>;

    pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TypeConfig>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
    pub type InitializeError = openraft::error::InitializeError<TypeConfig>;

    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

pub fn encode<T: serde::Serialize>(t: T) -> String {
    serde_json::to_string(&t).unwrap()
}

pub fn decode<T: serde::de::DeserializeOwned>(s: &str) -> T {
    serde_json::from_str(s).unwrap()
}

pub async fn new_raft(node_id: NodeId, router: Router) -> (typ::Raft, App) {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };
    let config = Arc::new(config.validate().unwrap());
    let (log_store, state_machine_store) = store::new("test.db").await;
    let raft = Raft::new(
        node_id,
        config,
        router.clone(),
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();
    let app = App::new(node_id, raft.clone(), router, Arc::new(state_machine_store));
    (raft, app)
}
