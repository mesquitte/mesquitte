mod api;
mod app;
pub mod client;
pub mod error;
mod network;
mod pool;
pub mod store;

use std::{fmt::Display, net::SocketAddr, path::Path, sync::Arc};

use app::App;
use network::Network;
use openraft::{Config, Raft};
use pool::ClientPool;
use serde::{Deserialize, Serialize};
use store::{Request, Response, StateMachineData, log_store};

pub type NodeId = u64;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub rpc_addr: String,
    pub api_addr: String,
}

impl Node {
    pub fn new(rpc_addr: &str, api_addr: &str) -> Self {
        Self {
            rpc_addr: rpc_addr.to_string(),
            api_addr: api_addr.to_string(),
        }
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Node rpc_addr: {}, api_addr: {} ",
            self.rpc_addr, self.api_addr
        )
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        Node = Node,
        SnapshotData = StateMachineData,
);

pub type LogStore<C> = log_store::LogStore<C>;
pub type StateMachineStore = store::StateMachineStore;

pub mod typ {

    use super::TypeConfig;

    pub type Raft = openraft::Raft<TypeConfig>;

    pub type Vote = <TypeConfig as openraft::RaftTypeConfig>::Vote;
    pub type LeaderId = <TypeConfig as openraft::RaftTypeConfig>::LeaderId;
    pub type LogId = openraft::LogId<TypeConfig>;
    pub type Entry = openraft::Entry<TypeConfig>;
    pub type EntryPayload = openraft::EntryPayload<TypeConfig>;
    pub type StoredMembership = openraft::StoredMembership<TypeConfig>;

    pub type Node = <TypeConfig as openraft::RaftTypeConfig>::Node;

    pub type LogState = openraft::storage::LogState<TypeConfig>;

    pub type SnapshotMeta = openraft::SnapshotMeta<TypeConfig>;
    pub type Snapshot = openraft::Snapshot<TypeConfig>;
    pub type SnapshotData = <TypeConfig as openraft::RaftTypeConfig>::SnapshotData;

    pub type IOFlushed = openraft::storage::IOFlushed<TypeConfig>;

    pub type Infallible = openraft::error::Infallible;
    pub type Fatal = openraft::error::Fatal<TypeConfig>;
    pub type RaftError<E = openraft::error::Infallible> = openraft::error::RaftError<TypeConfig, E>;
    pub type RPCError<E = openraft::error::Infallible> = openraft::error::RPCError<TypeConfig, E>;

    pub type ErrorSubject = openraft::ErrorSubject<TypeConfig>;
    pub type StorageError = openraft::StorageError<TypeConfig>;
    pub type StreamingError = openraft::error::StreamingError<TypeConfig>;

    pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;

    pub type ClientWriteError = openraft::error::ClientWriteError<TypeConfig>;
    pub type CheckIsLeaderError = openraft::error::CheckIsLeaderError<TypeConfig>;
    pub type ForwardToLeader = openraft::error::ForwardToLeader<TypeConfig>;
    pub type InitializeError = openraft::error::InitializeError<TypeConfig>;

    pub type VoteRequest = openraft::raft::VoteRequest<TypeConfig>;
    pub type VoteResponse = openraft::raft::VoteResponse<TypeConfig>;
    pub type AppendEntriesRequest = openraft::raft::AppendEntriesRequest<TypeConfig>;
    pub type AppendEntriesResponse = openraft::raft::AppendEntriesResponse<TypeConfig>;
    pub type InstallSnapshotRequest = openraft::raft::InstallSnapshotRequest<TypeConfig>;
    pub type InstallSnapshotResponse = openraft::raft::InstallSnapshotResponse<TypeConfig>;
    pub type SnapshotResponse = openraft::raft::SnapshotResponse<TypeConfig>;
    pub type ClientWriteResponse = openraft::raft::ClientWriteResponse<TypeConfig>;
}

pub async fn new_raft<P: AsRef<Path>>(
    node_id: NodeId,
    rpc_addr: SocketAddr,
    api_addr: SocketAddr,
    dir: P,
) -> (typ::Raft, App) {
    let config = Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_in_snapshot_log_to_keep: 0,
        ..Default::default()
    };

    let config = Arc::new(config.validate().unwrap());

    let (log_store, state_machine_store) = store::new(dir).await;
    let client_poll = ClientPool::new(10);
    let network = Network::new(node_id, client_poll);
    let raft = Raft::new(
        node_id,
        config,
        network,
        log_store,
        state_machine_store.clone(),
    )
    .await
    .unwrap();

    let app = App::new(
        node_id,
        rpc_addr,
        api_addr,
        raft.clone(),
        state_machine_store,
    );

    (raft, app)
}
