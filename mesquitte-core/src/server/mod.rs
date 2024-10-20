use std::sync::Arc;

use state::GlobalState;
use tokio::io::{split, AsyncRead, AsyncWrite};

use crate::{
    protocols,
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

async fn process_client<S, T>(stream: S, global: Arc<GlobalState>, storage: Arc<Storage<T>>)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    let (rd, wr) = split(stream);
    cfg_if::cfg_if! {
        if #[cfg(feature="v4")] {
            protocols::v4::read_write_loop::read_write_loop(rd, wr, global, storage).await;
        } else if #[cfg(feature="v5")] {
            protocols::v5::read_write_loop::read_write_loop(rd, wr, global, storage).await;
        }
    }
}
