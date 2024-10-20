use std::{path::Path, sync::Arc};

use tokio::net::{TcpListener, ToSocketAddrs};

use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "mqtts")]
use {crate::server::config::TlsConfig, crate::server::rustls::rustls_acceptor, std::path::Path};

use super::Error;

pub struct TcpServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    inner: TcpListener,
    config: ServerConfig<P>,
    global: Arc<GlobalState>,
    storage: Arc<Storage<S>>,
}

impl<P, S> TcpServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        config: ServerConfig<P>,
        global: Arc<GlobalState>,
        storage: Arc<Storage<S>>,
    ) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            inner: listener,
            config,
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
    pub async fn accept_tls<P: AsRef<Path>>(&self, tls: &TlsConfig<P>) -> Result<(), Error> {
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
