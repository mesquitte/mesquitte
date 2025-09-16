use std::{io, num::ParseIntError};

use mqtt_codec_kit::common::{ProtocolLevel, protocol_level::ProtocolLevelError};
use state::GlobalState;
use tokio::io::{AsyncRead, AsyncWrite, split};

#[cfg(feature = "v4")]
use crate::protocols::v4;
#[cfg(feature = "v5")]
use crate::protocols::v5;
use crate::{
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
    warn,
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] io::Error),
    #[error("Parse protocol level error : {0}")]
    WrongConfig(#[from] ParseIntError),
    #[error("Wrong protocol level set : {0}")]
    ProtocolLevel(#[from] ProtocolLevelError),
    #[cfg(any(feature = "ws", feature = "wss"))]
    #[error("tungstenite Error : {0}")]
    Accept(#[from] Box<tungstenite::Error>),
    #[error("Missing tls config")]
    MissingTlsConfig,
    #[cfg(feature = "rustls")]
    #[error("Wrong tls config: {0}")]
    Rustls(#[from] crate::server::rustls::Error),
    #[error("Unsupport Protocol Level: {0}")]
    UnsupportProtocol(String),
    #[cfg(feature = "quic")]
    #[error("Infallible Error")]
    Infallible(#[from] std::convert::Infallible),
    #[cfg(feature = "quic")]
    #[error("Quic Start Error")]
    StartError(#[from] s2n_quic::provider::StartError),
    #[cfg(feature = "quic")]
    #[error("QuicServer Connect Error : {0}")]
    Connection(#[from] s2n_quic::connection::Error),
    #[cfg(all(unix, feature = "quic"))]
    #[error("QuicServer Tls Error : {0}")]
    QuicTls(#[from] s2n_quic::provider::tls::default::error::Error),
    #[cfg(all(windows, feature = "quic"))]
    #[error("QuicServer Tls Error : {0}")]
    QuicTls(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[cfg(feature = "quic")]
    #[error("Connection broken")]
    ConnectionBroken,
}

async fn process_client<S, T>(
    stream: S,
    level: ProtocolLevel,
    global: &'static GlobalState<T>,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Send + Sync + 'static,
    T: MessageStore + RetainMessageStore + TopicStore,
{
    let (rd, wr) = split(stream);
    match level {
        ProtocolLevel::Version310 | ProtocolLevel::Version311 => {
            if cfg!(feature = "v5") && !cfg!(feature = "v4") {
                warn!("this broker does not support v4");
                return Err(Error::UnsupportProtocol("v4".to_string()));
            }
            #[cfg(feature = "v4")]
            v4::EventLoop::new(rd, wr, global).run().await;
        }
        ProtocolLevel::Version50 => {
            if cfg!(feature = "v4") && !cfg!(feature = "v5") {
                warn!("this broker does not support v5");
                return Err(Error::UnsupportProtocol("v5".to_string()));
            }
            #[cfg(feature = "v5")]
            v5::EventLoop::new(rd, wr, global).run().await;
        }
    }
    Ok(())
}
