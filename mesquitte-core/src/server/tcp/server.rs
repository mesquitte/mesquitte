use std::{net::SocketAddr, sync::Arc};

use tokio::net::TcpListener;

#[cfg(feature = "mqtts")]
use crate::server::config::TlsConfig;
use crate::{
    server::{process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

use super::Error;

pub struct TcpServer<MS, RS, TS>
where
    MS: MessageStore + Sync + Send + 'static,
    RS: RetainMessageStore + Sync + Send + 'static,
    TS: TopicStore + Sync + Send + 'static,
{
    inner: TcpListener,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
}

impl<MS, RS, TS> TcpServer<MS, RS, TS>
where
    MS: MessageStore + Sync + Send + 'static,
    RS: RetainMessageStore + Sync + Send + 'static,
    TS: TopicStore + Sync + Send + 'static,
{
    pub async fn bind(
        addr: SocketAddr,
        global: Arc<GlobalState>,
        storage: Arc<Storage<MS, RS, TS>>,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            inner: listener,
            global,
            storage,
        })
    }

    #[cfg(feature = "mqtt")]
    pub async fn accept(&self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            let storage = self.storage.clone();
            tokio::spawn(async move {
                process_client(stream, global, storage).await;
            });
        }
        Ok(())
    }

    #[cfg(feature = "mqtts")]
    pub async fn accept_tls(&self, tls: &TlsConfig) -> Result<(), Error> {
        use crate::server::rustls::rustls_acceptor;

        let acceptor = rustls_acceptor(tls)?;
        while let Ok((stream, _addr)) = self.inner.accept().await {
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    let global = self.global.clone();
                    tokio::spawn(async move { process_client(stream, global).await });
                }
                Err(err) => {
                    log::warn!("accept tls stream failed: {err}");
                    continue;
                }
            }
        }
        Ok(())
    }
}
