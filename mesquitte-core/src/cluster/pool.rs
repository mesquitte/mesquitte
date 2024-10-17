use std::net::SocketAddr;

use dashmap::DashMap;
use log::info;
use mobc::{async_trait, Connection, Manager, Pool};
use tarpc::{client::Config, serde_transport::Transport, tokio_serde::formats::Bincode};
use tokio::net::TcpStream;

use super::{app::RaftRPCClient, error::Error};

pub struct RPCClientManager {
    pub addr: SocketAddr,
}

impl RPCClientManager {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }
}

#[async_trait]
impl Manager for RPCClientManager {
    type Connection = RaftRPCClient;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        info!("Raft NetworkConnection connecting to target: {}", self.addr);

        let stream = TcpStream::connect(&self.addr).await?;
        let transport = Transport::from((stream, Bincode::default()));
        let client_stub = RaftRPCClient::new(Config::default(), transport).spawn();
        Ok(client_stub)
    }

    async fn check(&self, conn: Self::Connection) -> Result<Self::Connection, Self::Error> {
        Ok(conn)
    }
}

#[derive(Clone)]
pub struct ClientPool {
    max_open_connection: u64,
    rpc_client_pool: DashMap<String, Pool<RPCClientManager>>,
}

impl ClientPool {
    pub fn new(max_open_connection: u64) -> Self {
        Self {
            max_open_connection,
            rpc_client_pool: DashMap::with_capacity(2),
        }
    }

    pub async fn make_rpc_connection(
        &self,
        addr: SocketAddr,
    ) -> Result<Connection<RPCClientManager>, Error> {
        let key = addr.to_string();
        if !self.rpc_client_pool.contains_key(&key) {
            let manager = RPCClientManager::new(addr);
            let pool = Pool::builder()
                .max_open(self.max_open_connection)
                .build(manager);
            self.rpc_client_pool.insert(key.clone(), pool);
        }
        if let Some(poll) = self.rpc_client_pool.get(&key) {
            match poll.get().await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    return Err(Error::NoAvailableRaftRPCClient(
                        addr.to_string(),
                        e.to_string(),
                    ));
                }
            };
        }
        Err(Error::NoAvailableRaftRPCClient(
            addr.to_string(),
            "connection pool could not established".to_string(),
        ))
    }
}
