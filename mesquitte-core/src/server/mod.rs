use std::sync::Arc;

use state::GlobalState;
use tokio::io::{split, AsyncRead, AsyncWrite};

use crate::protocols::v5::read_write_loop::read_write_loop;

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

async fn process_client<S>(stream: S, global: Arc<GlobalState>)
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (rd, wr) = split(stream);
    read_write_loop(rd, wr, global).await;
}
