use std::{net::SocketAddr, sync::Arc};

use s2n_quic::{provider::tls, Server};

use crate::server::{process_client, state::GlobalState};

use super::Error;

pub struct QuicServer {
    inner: Server,
    global: Arc<GlobalState>,
}

impl QuicServer {
    pub fn bind<T: tls::TryInto>(
        addr: SocketAddr,
        tls: T,
        global: Arc<GlobalState>,
    ) -> Result<Self, Error>
    where
        Error: From<<T as tls::TryInto>::Error>,
    {
        let server = Server::builder().with_tls(tls)?.with_io(addr)?.start()?;
        Ok(QuicServer {
            inner: server,
            global,
        })
    }

    pub async fn accept(mut self) -> Result<(), Error> {
        while let Some(mut connection) = self.inner.accept().await {
            let g = self.global.clone();
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                    process_client(stream, g.clone()).await;
                }
            });
        }
        Ok(())
    }
}
