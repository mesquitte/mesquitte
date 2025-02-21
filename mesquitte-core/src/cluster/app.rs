use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    sync::Arc,
};

use axum::{
    Router,
    routing::{get, post},
};
use futures::{future, prelude::*};
use log::info;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use tarpc::{
    context::Context,
    server::{BaseChannel, Channel as _, incoming::Incoming as _},
    tokio_serde::formats::Bincode,
};

use crate::cluster::api::*;

use super::{
    Node, NodeId, StateMachineStore, TypeConfig,
    store::Request,
    typ::{
        ClientWriteError, ClientWriteResponse, InitializeError, Raft, RaftError, RaftMetrics,
        Snapshot, SnapshotData, SnapshotMeta, Vote,
    },
};

#[tarpc::service]
pub trait RaftRPC {
    async fn read(args: String) -> Option<String>;
    async fn write(args: Request) -> Result<ClientWriteResponse, RaftError<ClientWriteError>>;
    async fn init() -> Result<(), RaftError<InitializeError>>;
    async fn add_learner(
        args: (NodeId, String, String),
    ) -> Result<ClientWriteResponse, RaftError<ClientWriteError>>;
    async fn change_membership(
        args: BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, RaftError<ClientWriteError>>;
    async fn metrics() -> RaftMetrics;
    async fn append(args: AppendEntriesRequest<TypeConfig>) -> AppendEntriesResponse<TypeConfig>;
    async fn snapshot(
        vote: Vote,
        snapshot_meta: SnapshotMeta,
        snapshot_data: SnapshotData,
    ) -> SnapshotResponse<TypeConfig>;
    async fn vote(args: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig>;
}

#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    pub rpc_addr: SocketAddr,
    pub api_addr: SocketAddr,
    pub raft: Raft,
    pub state_machine_store: Arc<StateMachineStore>,
}

impl App {
    pub fn new(
        id: NodeId,
        rpc_addr: SocketAddr,
        api_addr: SocketAddr,
        raft: Raft,
        state_machine_store: Arc<StateMachineStore>,
    ) -> Self {
        Self {
            id,
            rpc_addr,
            api_addr,
            raft,
            state_machine_store,
        }
    }

    pub async fn run(&self) {
        let api_addr = self.api_addr;
        let this = self.clone();
        tokio::spawn(async move {
            let app = Router::new()
                .route("/read", post(read))
                .route("/write", post(write))
                .route("/learner", post(add_learner))
                .route("/membership", post(change_membership))
                .route("/init", post(init))
                .route("/metrics", get(metrics))
                .with_state(this);
            let listener = tokio::net::TcpListener::bind(&api_addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        let mut listener = tarpc::serde_transport::tcp::listen(&self.rpc_addr, Bincode::default)
            .await
            .unwrap();
        info!("Listening on port {}", listener.local_addr().port());
        listener.config_mut().max_frame_length(8 * 1024 * 1024);
        listener
            .filter_map(|r| future::ready(r.ok()))
            .map(BaseChannel::with_defaults)
            .max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
            .map(|channel| channel.execute(self.clone().serve()).for_each(Self::spawn))
            .buffer_unordered(200)
            .for_each(|_| async {})
            .await;
    }

    async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(fut);
    }
}

impl RaftRPC for App {
    async fn read(self, _: Context, args: String) -> Option<String> {
        let state_machine = self.state_machine_store.sm.read();
        state_machine.data.get(&args).cloned()
    }

    async fn write(
        self,
        _: Context,
        args: Request,
    ) -> Result<ClientWriteResponse, RaftError<ClientWriteError>> {
        self.raft.client_write(args).await
    }

    async fn init(self, _: Context) -> Result<(), RaftError<InitializeError>> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            self.id,
            Node {
                rpc_addr: self.rpc_addr.to_string(),
                api_addr: self.api_addr.to_string(),
            },
        );
        self.raft.initialize(nodes).await
    }

    async fn add_learner(
        self,
        _: Context,
        args: (NodeId, String, String),
    ) -> Result<ClientWriteResponse, RaftError<ClientWriteError>> {
        let node = Node {
            rpc_addr: args.1,
            api_addr: args.2,
        };
        self.raft.add_learner(args.0, node, true).await
    }

    async fn change_membership(
        self,
        _: Context,
        args: BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, RaftError<ClientWriteError>> {
        self.raft.change_membership(args, false).await
    }

    async fn metrics(self, _: Context) -> RaftMetrics {
        self.raft.metrics().borrow().clone()
    }

    async fn append(
        self,
        _: Context,
        args: AppendEntriesRequest<TypeConfig>,
    ) -> AppendEntriesResponse<TypeConfig> {
        self.raft.append_entries(args).await.unwrap()
    }

    async fn snapshot(
        self,
        _: Context,
        vote: Vote,
        snapshot_meta: SnapshotMeta,
        snapshot_data: SnapshotData,
    ) -> SnapshotResponse<TypeConfig> {
        let snapshot = Snapshot {
            meta: snapshot_meta,
            snapshot: snapshot_data,
        };
        self.raft
            .install_full_snapshot(vote, snapshot)
            .await
            .unwrap()
    }

    async fn vote(self, _: Context, args: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig> {
        self.raft.vote(args).await.unwrap()
    }
}
