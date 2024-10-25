use std::{net::SocketAddr, num::NonZeroUsize, path::Path};

use async_tungstenite::{accept_hdr_async, tokio::TokioAdapter};
use tokio::net::TcpSocket;
#[cfg(any(feature = "ws", feature = "wss"))]
use tungstenite::{handshake::server::ErrorResponse, http};

use crate::{
    info,
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "wss")]
use crate::{server::config::TlsConfig, server::rustls::rustls_acceptor, warn};

use super::ws_stream::WsByteStream;

pub struct WsServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> WsServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub async fn new(
        config: ServerConfig<P>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Result<Self, Error> {
        Ok(Self {
            config,
            global,
            storage,
        })
    }

    #[cfg(feature = "ws")]
    pub async fn run(self) -> Result<(), Error> {
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("ws woker {} initial", i);
            let socket = match self.config.addr {
                SocketAddr::V4(_) => TcpSocket::new_v4()?,
                SocketAddr::V6(_) => TcpSocket::new_v6()?,
            };
            socket.set_reuseport(true)?;
            socket.bind(self.config.addr)?;
            let listener = socket.listen(1024)?;
            let task = tokio::spawn(async move {
                while let Ok((stream, _addr)) = listener.accept().await {
                    let ws_stream = WsByteStream::new(
                        accept_hdr_async(TokioAdapter::new(stream), ws_callback).await?,
                    );
                    tokio::spawn(async move {
                        process_client(ws_stream, self.config.version, self.global, self.storage)
                            .await?;
                        Ok::<(), Error>(())
                    });
                }
                Ok::<(), Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            let _ = task.await;
        }
        Ok(())
    }

    #[cfg(feature = "wss")]
    pub async fn accept_tls(self, tls: &TlsConfig<P>) -> Result<(), Error> {
        let acceptor = rustls_acceptor(tls)?;
        while let Ok((stream, _addr)) = self.inner.accept().await {
            match acceptor.accept(stream).await {
                Ok(stream) => {
                    let ws_stream = WsByteStream::new(
                        accept_hdr_async(TokioAdapter::new(stream), ws_callback).await?,
                    );
                    tokio::spawn(async move {
                        process_client(ws_stream, self.config.version, self.global, self.storage)
                            .await?;
                        Ok::<(), Error>(())
                    });
                }
                Err(err) => {
                    warn!("accept WebSocket tls stream failed: {err}");
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
