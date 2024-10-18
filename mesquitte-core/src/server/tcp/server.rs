use std::sync::Arc;

use tokio::net::{TcpListener, ToSocketAddrs};

use crate::server::{process_client, state::GlobalState};
#[cfg(feature = "mqtts")]
use {crate::server::config::TlsConfig, crate::server::rustls::rustls_acceptor, std::path::Path};

use super::Error;

pub struct TcpServer {
    inner: TcpListener,
    global: Arc<GlobalState>,
}

impl TcpServer {
    pub async fn bind<A: ToSocketAddrs>(addr: A, global: Arc<GlobalState>) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            inner: listener,
            global,
        })
    }

    #[cfg(feature = "mqtt")]
    pub async fn accept(&self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            tokio::spawn(async move {
                process_client(stream, global).await;
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
