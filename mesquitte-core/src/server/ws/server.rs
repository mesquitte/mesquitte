use std::{net::SocketAddr, sync::Arc};

use async_tungstenite::{accept_hdr_async, tokio::TokioAdapter};
use tokio::net::TcpListener;

#[cfg(feature = "wss")]
use crate::server::config::TlsConfig;
use crate::server::{process_client, state::GlobalState};

use super::{ws_callback::ws_callback, ws_stream::WsByteStream, Error};

pub struct WsServer {
    inner: TcpListener,
    global: Arc<GlobalState>,
}

impl WsServer {
    pub async fn bind(addr: SocketAddr, global: Arc<GlobalState>) -> Result<Self, Error> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            inner: listener,
            global,
        })
    }

    #[cfg(feature = "ws")]
    pub async fn accept(&self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            let ws_stream = WsByteStream::new(
                accept_hdr_async(TokioAdapter::new(stream), ws_callback)
                    .await
                    .expect("WebSocket tls handshake"),
            );
            tokio::spawn(async move { process_client(ws_stream, global).await });
        }
        Ok(())
    }

    #[cfg(feature = "wss")]
    pub async fn accept_tls(&self, tls: &TlsConfig) -> Result<(), Error> {
        use crate::server::rustls::rustls_acceptor;

        let acceptor = rustls_acceptor(tls)?;
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    let ws_stream = WsByteStream::new(
                        accept_hdr_async(TokioAdapter::new(stream), ws_callback)
                            .await
                            .expect("WebSocket tls handshake"),
                    );
                    tokio::spawn(async move { process_client(ws_stream, global).await });
                }
                Err(err) => {
                    log::error!("accept WebSocket tls stream failed: {err}");
                    continue;
                }
            }
        }
        Ok(())
    }
}
