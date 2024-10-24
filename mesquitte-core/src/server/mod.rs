use mqtt_codec_kit::common::ProtocolLevel;
use state::GlobalState;
use tokio::io::{split, AsyncRead, AsyncWrite};

#[cfg(feature = "v4")]
use crate::protocols::v4;
#[cfg(feature = "v5")]
use crate::protocols::v5;
use crate::{
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
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
    Io(#[from] std::io::Error),
    #[cfg(any(feature = "ws", feature = "wss"))]
    #[error("tungstenite Error : {0}")]
    Accept(#[from] tungstenite::Error),
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
    #[cfg(feature = "quic")]
    #[error("Connection broken")]
    ConnectionBroken,
    #[cfg(feature = "v4")]
    #[error(transparent)]
    V4VariablePacket(#[from] mqtt_codec_kit::v4::packet::VariablePacketError),
    #[cfg(feature = "v5")]
    #[error(transparent)]
    V5VariablePacket(#[from] mqtt_codec_kit::v5::packet::VariablePacketError),
}

async fn process_client<S, T>(
    stream: S,
    level: ProtocolLevel,
    global: &'static GlobalState,
    storage: &'static Storage<T>,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
    T: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    let (rd, wr) = split(stream);
    match level {
        ProtocolLevel::Version310 | ProtocolLevel::Version311 => {
            if cfg!(feature = "v5") && !cfg!(feature = "v4") {
                warn!("this broker does not support v4");
                return Err(Error::UnsupportProtocol("v4".to_string()));
            }
            #[cfg(feature = "v4")]
            v4::EventLoop::new(rd, wr, global, storage).run().await;
        }
        ProtocolLevel::Version50 => {
            if cfg!(feature = "v4") && !cfg!(feature = "v5") {
                warn!("this broker does not support v5");
                return Err(Error::UnsupportProtocol("v5".to_string()));
            }
            #[cfg(feature = "v5")]
            v5::read_write_loop::read_write_loop(rd, wr, global, storage).await
        }
    }
    Ok(())
}
