use std::collections::BTreeSet;

use log::info;
use openraft::error::NetworkError;
use serde::{Deserialize, Serialize};
use tarpc::{client::Config, context, serde_transport::Transport, tokio_serde::formats::Bincode};
use tokio::net::TcpStream;

use super::{
    NodeId,
    app::RaftRPCClient,
    store::Request,
    typ::{ClientWriteResponse, ForwardToLeader, RPCError, RaftMetrics},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Empty {}

pub struct ClusterClient {
    pub leader_id: NodeId,
    pub inner: RaftRPCClient,
}

impl ClusterClient {
    pub async fn new(leader_id: NodeId, leader_addr: String) -> Self {
        let stream = TcpStream::connect(leader_addr).await.unwrap();
        let transport = Transport::from((stream, Bincode::default()));
        let client_stub = RaftRPCClient::new(Config::default(), transport).spawn();
        Self {
            leader_id,
            inner: client_stub,
        }
    }

    pub async fn write(&mut self, req: &Request) -> Result<ClientWriteResponse, RPCError> {
        let mut n_retry = 3;
        loop {
            match self.inner.write(context::current(), req.to_owned()).await {
                Ok(r) => match r {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        if let Some(ForwardToLeader {
                            leader_id: Some(leader_id),
                            leader_node: Some(leader_node),
                            ..
                        }) = e.forward_to_leader()
                        {
                            info!("new leader {} : {}", leader_id, leader_node);
                            let stream = TcpStream::connect(&leader_node.rpc_addr).await.unwrap();
                            let transport = Transport::from((stream, Bincode::default()));
                            let client_stub =
                                RaftRPCClient::new(Config::default(), transport).spawn();
                            self.inner = client_stub;
                            n_retry -= 1;
                            if n_retry > 0 {
                                continue;
                            }
                        } else {
                            return Err(RPCError::Network(NetworkError::new(&e)));
                        }
                    }
                },
                Err(e) => return Err(RPCError::Network(NetworkError::new(&e))),
            };
        }
    }

    pub async fn read(&self, req: &String) -> Result<Option<String>, RPCError> {
        self.inner
            .read(context::current(), req.to_owned())
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }

    pub async fn init(&self) -> Result<(), RPCError> {
        match self.inner.init(context::current()).await {
            Ok(r) => match r {
                Ok(v) => Ok(v),
                Err(e) => Err(RPCError::Network(NetworkError::new(&e))),
            },
            Err(e) => Err(RPCError::Network(NetworkError::new(&e))),
        }
    }

    pub async fn add_learner(
        &mut self,
        req: (NodeId, String, String),
    ) -> Result<ClientWriteResponse, RPCError> {
        let mut n_retry = 3;
        loop {
            match self
                .inner
                .add_learner(context::current(), req.to_owned())
                .await
            {
                Ok(r) => match r {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        if let Some(ForwardToLeader {
                            leader_id: Some(leader_id),
                            leader_node: Some(leader_node),
                            ..
                        }) = e.forward_to_leader()
                        {
                            info!("new leader {} : {}", leader_id, leader_node);
                            let stream = TcpStream::connect(&leader_node.rpc_addr).await.unwrap();
                            let transport = Transport::from((stream, Bincode::default()));
                            let client_stub =
                                RaftRPCClient::new(Config::default(), transport).spawn();
                            self.inner = client_stub;
                            n_retry -= 1;
                            if n_retry > 0 {
                                continue;
                            }
                        } else {
                            return Err(RPCError::Network(NetworkError::new(&e)));
                        }
                    }
                },
                Err(e) => return Err(RPCError::Network(NetworkError::new(&e))),
            };
        }
    }

    pub async fn change_membership(
        &mut self,
        req: &BTreeSet<NodeId>,
    ) -> Result<ClientWriteResponse, RPCError> {
        let mut n_retry = 3;
        loop {
            match self
                .inner
                .change_membership(context::current(), req.to_owned())
                .await
            {
                Ok(r) => match r {
                    Ok(v) => return Ok(v),
                    Err(e) => {
                        if let Some(ForwardToLeader {
                            leader_id: Some(leader_id),
                            leader_node: Some(leader_node),
                            ..
                        }) = e.forward_to_leader()
                        {
                            info!("new leader {} : {}", leader_id, leader_node);
                            let stream = TcpStream::connect(&leader_node.rpc_addr).await.unwrap();
                            let transport = Transport::from((stream, Bincode::default()));
                            let client_stub =
                                RaftRPCClient::new(Config::default(), transport).spawn();
                            self.inner = client_stub;
                            n_retry -= 1;
                            if n_retry > 0 {
                                continue;
                            }
                        } else {
                            return Err(RPCError::Network(NetworkError::new(&e)));
                        }
                    }
                },
                Err(e) => return Err(RPCError::Network(NetworkError::new(&e))),
            };
        }
    }

    pub async fn metrics(&self) -> Result<RaftMetrics, RPCError> {
        self.inner
            .metrics(context::current())
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}
