use std::net::SocketAddr;

use axum::{
    routing::{get, post},
    Router,
};
use futures::{future, prelude::*};
use log::info;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use tarpc::{
    context::Context,
    server::{incoming::Incoming as _, BaseChannel, Channel as _},
    tokio_serde::formats::Bincode,
};

use crate::cluster::api::*;

use super::{typ, NodeId, TypeConfig};

#[tarpc::service]
pub trait RaftRPC {
    async fn append(args: AppendEntriesRequest<TypeConfig>) -> AppendEntriesResponse<TypeConfig>;
    async fn snapshot(
        vote: typ::Vote,
        snapshot_meta: typ::SnapshotMeta,
        snapshot_data: typ::SnapshotData,
    ) -> SnapshotResponse<TypeConfig>;
    async fn vote(args: VoteRequest<TypeConfig>) -> VoteResponse<TypeConfig>;
}

#[derive(Clone)]
pub struct App {
    pub id: NodeId,
    server_addr: SocketAddr,
    pub raft: typ::Raft,
}

impl App {
    pub fn new(id: NodeId, server_addr: SocketAddr, raft: typ::Raft) -> Self {
        Self {
            id,
            server_addr,
            raft,
        }
    }

    pub async fn run(&self) {
        let addr = self.server_addr;
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
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app).await.unwrap();
        });

        let mut listener = tarpc::serde_transport::tcp::listen(addr, Bincode::default)
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
        vote: typ::Vote,
        snapshot_meta: typ::SnapshotMeta,
        snapshot_data: typ::SnapshotData,
    ) -> SnapshotResponse<TypeConfig> {
        let snapshot = typ::Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(snapshot_data),
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
