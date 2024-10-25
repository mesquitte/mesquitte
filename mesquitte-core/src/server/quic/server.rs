use std::{num::NonZeroUsize, path::Path};

use s2n_quic::Server;

use crate::{
    info,
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

pub struct QuicServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> QuicServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub fn new(
        config: ServerConfig<P>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Result<Self, Error> {
        Ok(QuicServer {
            config,
            global,
            storage,
        })
    }

    pub async fn run(self) -> Result<(), Error> {
        let worker = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let mut tasks = Vec::with_capacity(worker);
        for i in 0..worker {
            info!("quic woker {} initial", i);
            let tls = match &self.config.tls {
                Some(tls) => (tls.cert_file.as_ref(), tls.key_file.as_ref()),
                None => return Err(Error::MissingTlsConfig),
            };
            let tls = s2n_quic::provider::tls::default::Server::builder()
                .with_certificate(tls.0, tls.1)?
                .build()?;
            let io = s2n_quic::provider::io::Default::builder()
                .with_receive_address(self.config.addr)?
                .with_reuse_port()?
                .build()?;
            let mut server = Server::builder().with_tls(tls)?.with_io(io)?.start()?;
            let task = tokio::spawn(async move {
                while let Some(mut connection) = server.accept().await {
                    tokio::spawn(async move {
                        while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await
                        {
                            match process_client(
                                stream,
                                self.config.version,
                                self.global,
                                self.storage,
                            )
                            .await
                            {
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
