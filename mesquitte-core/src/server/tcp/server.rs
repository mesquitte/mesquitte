use std::path::Path;

use tokio::net::{TcpListener, ToSocketAddrs};

use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "mqtts")]
use {crate::server::config::TlsConfig, crate::server::rustls::rustls_acceptor};

use super::Error;

pub struct TcpServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    inner: TcpListener,
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> TcpServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        config: ServerConfig<P>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
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
    pub async fn accept(self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let v = self.config.version.clone();
            tokio::spawn(async move {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "v4")] {
                        assert_eq!(v, "v4");
                        process_client(stream, "v4", self.global, self.storage).await;
                    } else if #[cfg(feature = "v5")] {
                        assert_eq!(v, "v5");
                        process_client(stream, "v5", self.global, self.storage).await;
                    }
                }
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
