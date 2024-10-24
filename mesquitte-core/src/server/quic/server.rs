use std::path::Path;

use s2n_quic::{
    provider::{io, tls},
    Server,
};

use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState, Error},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

pub struct QuicServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    inner: Server,
    config: ServerConfig<P>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<P, S> QuicServer<P, S>
where
    P: AsRef<Path>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub fn bind<T: tls::TryInto, A: io::TryInto>(
        addr: A,
        tls: T,
        config: ServerConfig<P>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Result<Self, Error>
    where
        Error: From<<T as tls::TryInto>::Error> + From<<A as io::TryInto>::Error>,
    {
        let server = Server::builder().with_tls(tls)?.with_io(addr)?.start()?;
        Ok(QuicServer {
            inner: server,
            config,
            global,
            storage,
        })
    }

    pub async fn accept(mut self) -> Result<(), Error> {
        while let Some(mut connection) = self.inner.accept().await {
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                    match process_client(stream, self.config.version, self.global, self.storage)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => return Err(e),
                    }
                }
                Ok(())
            });
        }
        Ok(())
    }
}
