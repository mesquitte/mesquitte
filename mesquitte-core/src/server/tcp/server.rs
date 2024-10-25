use std::{net::SocketAddr, num::NonZeroUsize, path::Path};

use tokio::net::TcpSocket;

use crate::{
    info,
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};
#[cfg(feature = "mqtts")]
use crate::{server::config::TlsConfig, server::rustls::rustls_acceptor, warn};

pub struct TcpServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> TcpServer<P, S>
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

    #[cfg(feature = "mqtt")]
    pub async fn run(self) -> Result<(), Error> {
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("tcp woker {} initial", i);
            let socket = match self.config.addr {
                SocketAddr::V4(_) => TcpSocket::new_v4()?,
                SocketAddr::V6(_) => TcpSocket::new_v6()?,
            };
            socket.set_reuseport(true)?;
            socket.bind(self.config.addr)?;
            let listener = socket.listen(1024)?;
            let task = tokio::spawn(async move {
                while let Ok((stream, _addr)) = listener.accept().await {
                    tokio::spawn(async move {
                        process_client(stream, self.config.version, self.global, self.storage)
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
