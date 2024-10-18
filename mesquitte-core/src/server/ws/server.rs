use std::sync::Arc;

use async_tungstenite::{accept_hdr_async, tokio::TokioAdapter};
use tokio::net::{TcpListener, ToSocketAddrs};
#[cfg(any(feature = "ws", feature = "wss"))]
use tungstenite::{handshake::server::ErrorResponse, http};

use crate::{
    server::{process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "wss")]
use {crate::server::config::TlsConfig, crate::server::rustls::rustls_acceptor, std::path::Path};

use super::{ws_stream::WsByteStream, Error};

pub struct WsServer<MS, RS, TS>
where
    MS: MessageStore + Sync + Send + 'static,
    RS: RetainMessageStore + Sync + Send + 'static,
    TS: TopicStore + Sync + Send + 'static,
{
    inner: TcpListener,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
}

impl<MS, RS, TS> WsServer<MS, RS, TS>
where
    MS: MessageStore + Sync + Send + 'static,
    RS: RetainMessageStore + Sync + Send + 'static,
    TS: TopicStore + Sync + Send + 'static,
{
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
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

    #[cfg(feature = "ws")]
    pub async fn accept(&self) -> Result<(), Error> {
        while let Ok((stream, _addr)) = self.inner.accept().await {
            let global = self.global.clone();
            let storage = self.storage.clone();
            let ws_stream =
                WsByteStream::new(accept_hdr_async(TokioAdapter::new(stream), ws_callback).await?);
            tokio::spawn(async move { process_client(ws_stream, global, storage).await });
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
                    log::error!("accept WebSocket tls stream failed: {err}");
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
    if let Some(protocol) = req.headers().get("Sec-WebSocket-Protocol") {
        // see: [MQTT-6.0.0-3]
        if protocol != "mqtt" && protocol != "mqttv3.1" {
            log::info!("invalid WebSocket subprotocol name: {:?}", protocol);
            return Err(http::Response::new(Some(
                "invalid WebSocket subprotocol name".to_string(),
            )));
        }
        resp.headers_mut()
            .insert("Sec-WebSocket-Protocol", protocol.clone());
    }
    Ok(resp)
}
