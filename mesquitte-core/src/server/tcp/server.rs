use std::path::Path;

use tokio::net::{TcpListener, ToSocketAddrs};

#[cfg(feature = "mqtts")]
use crate::{server::config::TlsConfig, server::rustls::rustls_acceptor, warn};
use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

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
            tokio::spawn(async move {
                process_client(stream, self.config.version, self.global, self.storage).await?;
                Ok::<(), Error>(())
            });
        }
        Ok(())
    }

    #[cfg(feature = "mqtts")]
    pub async fn accept_tls(self, tls: &TlsConfig<P>) -> Result<(), Error> {
        let acceptor = rustls_acceptor(tls)?;
        while let Ok((stream, _addr)) = self.inner.accept().await {
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    tokio::spawn(async move {
                        process_client(stream, self.config.version, self.global, self.storage)
                            .await?;
                        Ok::<(), Error>(())
                    });
                }
                Err(err) => {
                    warn!("accept tls stream failed: {err}");
                    continue;
                }
            }
        }
        Ok(())
    }
}
