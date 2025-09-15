use std::net::SocketAddr;
#[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
use std::num::NonZeroUsize;

use tokio::net::TcpSocket;

use crate::{
    info,
    server::{Error, config::ServerConfig, process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
};
#[cfg(feature = "mqtts")]
use crate::{server::rustls::rustls_acceptor, warn};

pub struct TcpServer<S: 'static> {
    config: ServerConfig,
    global: &'static GlobalState<S>,
}

impl<S> TcpServer<S>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub async fn new(config: ServerConfig, global: &'static GlobalState<S>) -> Result<Self, Error> {
        Ok(Self { config, global })
    }

    #[cfg(feature = "mqtt")]
    pub async fn serve(self) -> Result<(), Error> {
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        #[cfg(any(target_os = "solaris", target_os = "illumos", target_os = "windows"))]
        let worker = 1;
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("tcp worker {} starting...", i);
            let socket = match self.config.addr {
                SocketAddr::V4(_) => TcpSocket::new_v4()?,
                SocketAddr::V6(_) => TcpSocket::new_v6()?,
            };
            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            socket.set_reuseport(true)?;
            socket.bind(self.config.addr)?;
            let listener = socket.listen(1024)?;
            let task = tokio::spawn(async move {
                while let Ok((stream, _addr)) = listener.accept().await {
                    tokio::spawn(async move {
                        process_client(stream, self.config.version, self.global).await?;
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
    pub async fn serve_tls(self) -> Result<(), Error> {
        let tls = match &self.config.tls {
            Some(tls) => tls,
            None => return Err(Error::MissingTlsConfig),
        };
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        #[cfg(any(target_os = "solaris", target_os = "illumos", target_os = "windows"))]
        let worker = 1;
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("tcp worker {} starting...", i);
            let acceptor = rustls_acceptor(tls)?;
            let socket = match self.config.addr {
                SocketAddr::V4(_) => TcpSocket::new_v4()?,
                SocketAddr::V6(_) => TcpSocket::new_v6()?,
            };
            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            socket.set_reuseport(true)?;
            socket.bind(self.config.addr)?;
            let listener = socket.listen(1024)?;
            let task = tokio::spawn(async move {
                while let Ok((stream, _addr)) = listener.accept().await {
                    match acceptor.accept(stream).await {
                        Ok(stream) => {
                            tokio::spawn(async move {
                                process_client(stream, self.config.version, self.global).await?;
                                Ok::<(), Error>(())
                            });
                        }
                        Err(err) => {
                            warn!("accept tls stream failed: {err}");
                            continue;
                        }
                    }
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
}
