use std::sync::Arc;

use s2n_quic::{
    provider::{io, tls},
    Server,
};

use crate::{
    server::{process_client, state::GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

use super::Error;

pub struct QuicServer<S>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    inner: Server,
    global: Arc<GlobalState>,
    storage: Arc<Storage<S>>,
}

impl<S> QuicServer<S>
where
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub fn bind<T: tls::TryInto, A: io::TryInto>(
        addr: A,
        tls: T,
        global: Arc<GlobalState>,
        storage: Arc<Storage<S>>,
    ) -> Result<Self, Error>
    where
        Error: From<<T as tls::TryInto>::Error> + From<<A as io::TryInto>::Error>,
    {
        let server = Server::builder().with_tls(tls)?.with_io(addr)?.start()?;
        Ok(QuicServer {
            inner: server,
            global,
            storage,
        })
    }

    pub async fn accept(mut self) -> Result<(), Error> {
        while let Some(mut connection) = self.inner.accept().await {
            let global = self.global.clone();
            let storage = self.storage.clone();
            tokio::spawn(async move {
                while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                    process_client(stream, global.clone(), storage.clone()).await;
                }
            });
        }
        Ok(())
    }
}
