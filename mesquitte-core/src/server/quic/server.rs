#[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
use std::num::NonZeroUsize;

use s2n_quic::Server;

use crate::{
    info,
    server::{Error, config::ServerConfig, process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
};

pub struct QuicServer<S: 'static> {
    config: ServerConfig,
    global: &'static GlobalState<S>,
}

impl<S> QuicServer<S>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(config: ServerConfig, global: &'static GlobalState<S>) -> Result<Self, Error> {
        Ok(QuicServer { config, global })
    }

    pub async fn serve(self) -> Result<(), Error> {
        #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        #[cfg(any(windows, target_os = "solaris", target_os = "illumos"))]
        let worker = 1;
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("quic worker {} staring...", i);
            let tls = match &self.config.tls {
                Some(tls) => (tls.cert_file.as_path(), tls.key_file.as_path()),
                None => return Err(Error::MissingTlsConfig),
            };
            let tls = s2n_quic::provider::tls::default::Server::builder()
                .with_certificate(tls.0, tls.1)?
                .build()?;
            #[cfg(all(unix, not(target_os = "solaris"), not(target_os = "illumos")))]
            let io = s2n_quic::provider::io::Default::builder()
                .with_receive_address(self.config.addr)?
                .with_reuse_port()?
                .build()?;
            #[cfg(any(windows, target_os = "solaris", target_os = "illumos"))]
            let io = s2n_quic::provider::io::Default::builder()
                .with_receive_address(self.config.addr)?
                .build()?;
            let mut server = Server::builder().with_tls(tls)?.with_io(io)?.start()?;
            let task = tokio::spawn(async move {
                while let Some(mut connection) = server.accept().await {
                    tokio::spawn(async move {
                        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await
                        {
                            match process_client(stream, self.config.version, self.global).await {
                                Ok(v) => v,
                                Err(e) => return Err(e),
                            }
                        }
                        Ok(())
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
}
