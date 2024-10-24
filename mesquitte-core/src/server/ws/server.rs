use std::path::Path;

use async_tungstenite::{accept_hdr_async, tokio::TokioAdapter};
use tokio::net::{TcpListener, ToSocketAddrs};
#[cfg(any(feature = "ws", feature = "wss"))]
use tungstenite::{handshake::server::ErrorResponse, http};

use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "wss")]
use {crate::server::config::TlsConfig, crate::server::rustls::rustls_acceptor, std::path::Path};

use super::ws_stream::WsByteStream;

pub struct WsServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    inner: TcpListener,
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> WsServer<P, S>
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

    #[cfg(feature = "ws")]
    pub async fn accept(self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let ws_stream =
                WsByteStream::new(accept_hdr_async(TokioAdapter::new(stream), ws_callback).await?);
            tokio::spawn(async move {
                process_client(ws_stream, self.config.version, self.global, self.storage).await?;
                Ok::<(), Error>(())
            });
        }
        Ok(())
    }

    #[cfg(feature = "wss")]
    pub async fn accept_tls<P: AsRef<Path>>(&self, tls: &TlsConfig<P>) -> Result<(), Error> {
        let acceptor = rustls_acceptor(tls)?;
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    let ws_stream = WsByteStream::new(
                        accept_hdr_async(TokioAdapter::new(stream), ws_callback).await?,
                    );
                    tokio::spawn(async move { process_client(ws_stream, global).await });
                }
                Err(err) => {
                    error!("accept WebSocket tls stream failed: {err}");
                    continue;
                }
            }
        }
        Ok(())
    }
}

#[allow(clippy::result_large_err)]
#[cfg(any(feature = "ws", feature = "wss"))]
pub fn ws_callback(
    req: &http::Request<()>,
    mut resp: http::Response<()>,
) -> Result<http::Response<()>, ErrorResponse> {
    use crate::info;

    if let Some(protocol) = req.headers().get("Sec-WebSocket-Protocol") {
        // see: [MQTT-6.0.0-3]
        if protocol != "mqtt" && protocol != "mqttv3.1" {
            info!("invalid WebSocket subprotocol name: {:?}", protocol);
            return Err(http::Response::new(Some(
                "invalid WebSocket subprotocol name".to_string(),
            )));
        }
        resp.headers_mut()
            .insert("Sec-WebSocket-Protocol", protocol.clone());
    }
    Ok(resp)
}
