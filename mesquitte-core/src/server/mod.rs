use std::sync::Arc;

use state::GlobalState;
use tokio::io::{split, AsyncRead, AsyncWrite};

use crate::{
    protocols::v4::read_write_loop::read_write_loop,
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

pub mod config;
#[cfg(feature = "quic")]
pub mod quic;
#[cfg(feature = "rustls")]
pub mod rustls;
pub mod state;
#[cfg(any(feature = "mqtt", feature = "mqtts"))]
pub mod tcp;
#[cfg(any(feature = "ws", feature = "wss"))]
pub mod ws;

async fn process_client<S, MS, RS, TS>(
    stream: S,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) where
    S: AsyncRead + AsyncWrite + Send + 'static,
    MS: MessageStore + Sync + Send + 'static,
    RS: RetainMessageStore + Sync + Send + 'static,
    TS: TopicStore + Sync + Send + 'static,
{
    let (rd, wr) = split(stream);
    read_write_loop(rd, wr, global, storage).await;
}
