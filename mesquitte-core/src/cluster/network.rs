use std::{future::Future, net::SocketAddr};

use backon::{ExponentialBuilder, Retryable};
use log::{info, warn};
use openraft::{
    error::{ReplicationClosed, Unreachable},
    network::{v2::RaftNetworkV2, RPCOption},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    },
    OptionalSend, RaftNetworkFactory, Snapshot, Vote,
};
use tarpc::{client::Config, context, serde_transport::Transport, tokio_serde::formats::Bincode};
use tokio::{net::TcpStream, sync::Mutex};

use super::{app::RaftRPCClient, error::Error, typ, Node, NodeId, TypeConfig};

pub struct Connection {
    client: Mutex<Option<RaftRPCClient>>,
    target: NodeId,
    addr: SocketAddr,
}

impl Connection {
    pub async fn new_client(&self) -> Result<RaftRPCClient, Error> {
        info!(
            "Raft NetworkConnection connecting to target: {}-{}",
            self.target, self.addr
        );
        let stream = TcpStream::connect(self.addr).await?;
        let transport = Transport::from((stream, Bincode::default()));
        let client_stub = RaftRPCClient::new(Config::default(), transport).spawn();
        Ok(client_stub)
    }

    async fn take_client(&mut self) -> Result<RaftRPCClient, Unreachable> {
        let mut client = self.client.lock().await;

        if let Some(c) = client.take() {
            return Ok(c);
        }

        let client_stub = (|| async { self.new_client().await })
            .retry(ExponentialBuilder::default())
            .sleep(tokio::time::sleep)
            .when(|e| e.to_string() == "EOF")
            .notify(|err, dur| {
                warn!("retrying {:?} after {:?}", err, dur);
            })
            .await
            .map_err(|e| Unreachable::new(&e))?;

        Ok(client_stub)
    }
}
pub struct Network {}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        let addr: SocketAddr = node.rpc_addr.parse().unwrap();
        Connection {
            client: Default::default(),
            target,
            addr,
        }
    }
}

impl RaftNetworkV2<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, typ::RPCError> {
        let client = self.take_client().await?;
        let resp = client.append(context::current(), req).await.unwrap();
        self.client.lock().await.replace(client);
        Ok(resp)
    }

    /// A real application should replace this method with customized implementation.
    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, typ::StreamingError> {
        let client = self.take_client().await?;
        let resp = client
            .snapshot(context::current(), vote, snapshot.meta, *snapshot.snapshot)
            .await
            .unwrap();
        self.client.lock().await.replace(client);
        Ok(resp)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, typ::RPCError> {
        let client = self.take_client().await?;
        let resp = client.vote(context::current(), req).await.unwrap();
        self.client.lock().await.replace(client);
        Ok(resp)
    }
}
