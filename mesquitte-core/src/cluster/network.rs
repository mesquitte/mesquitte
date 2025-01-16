use std::{future::Future, net::SocketAddr};

use backon::{ExponentialBuilder, Retryable};
use log::{info, warn};
use openraft::{
    error::{ReplicationClosed, Unreachable},
    network::{v2::RaftNetworkV2, RPCOption},
    OptionalSend, RaftNetworkFactory,
};
use tarpc::context;

use super::{
    pool::{ClientPool, RPCClientManager},
    typ::*,
    Node, NodeId, TypeConfig,
};

pub struct Connection {
    node_id: NodeId,
    target: NodeId,
    target_addr: SocketAddr,
    client_poll: ClientPool,
}

impl Connection {
    pub fn new(
        node_id: NodeId,
        target: NodeId,
        target_addr: SocketAddr,
        client_poll: &ClientPool,
    ) -> Self {
        Self {
            node_id,
            target,
            target_addr,
            client_poll: client_poll.clone(),
        }
    }

    async fn take_client(&mut self) -> Result<mobc::Connection<RPCClientManager>, Unreachable> {
        info!(
            "take client to target: {}-{}",
            self.target, self.target_addr
        );
        let client_stub =
            (|| async { self.client_poll.make_rpc_connection(self.target_addr).await })
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
pub struct Network {
    pub id: NodeId,
    pub client_poll: ClientPool,
}

impl Network {
    pub fn new(id: NodeId, client_poll: ClientPool) -> Self {
        Self { id, client_poll }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        info!("new client to target {}, addr {}", target, node.rpc_addr);
        let addr: SocketAddr = node.rpc_addr.parse().unwrap();
        Connection::new(self.id, target, addr, &self.client_poll)
    }
}

impl RaftNetworkV2<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse, RPCError> {
        info!("id:{} append entries take client", self.node_id);
        let client = self.take_client().await?;
        let resp = client.append(context::current(), req).await.unwrap();
        Ok(resp)
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote,
        snapshot: Snapshot,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse, StreamingError> {
        info!("id:{} full snapshot take client", self.node_id);
        let client = self.take_client().await?;
        let resp = client
            .snapshot(context::current(), vote, snapshot.meta, *snapshot.snapshot)
            .await
            .unwrap();
        Ok(resp)
    }

    async fn vote(
        &mut self,
        req: VoteRequest,
        _option: RPCOption,
    ) -> Result<VoteResponse, RPCError> {
        info!("id:{} vote take client", self.node_id);
        let client = self.take_client().await?;
        let resp = client.vote(context::current(), req).await.unwrap();
        Ok(resp)
    }
}
