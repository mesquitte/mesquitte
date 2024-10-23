use std::path::Path;

use s2n_quic::{
    provider::{io, tls},
    Server,
};

use crate::{
    server::{config::ServerConfig, process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

use super::Error;

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
            let v = self.config.version.clone();
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                    cfg_if::cfg_if! {
                        if #[cfg(feature = "v4")] {
                            assert_eq!(v, "v4");
                            process_client(stream, "v4", self.global, self.storage).await;
                        } else if #[cfg(feature = "v5")] {
                            assert_eq!(v, "v5");
                            process_client(stream, "v5", self.global, self.storage).await;
                        }
                    }
                }
            });
        }
        Ok(())
    }
}
