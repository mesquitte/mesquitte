use std::convert::Infallible;

pub mod server;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("Infallible Error")]
    Infallible(#[from] Infallible),
    #[error("Quic Start Error")]
    StartError(#[from] s2n_quic::provider::StartError),
    #[error("QuicServer Connect Error : {0}")]
    Connection(#[from] s2n_quic::connection::Error),
    #[error("Connection broken")]
    ConnectionBroken,
    #[error(transparent)]
    V4VariablePacket(#[from] mqtt_codec_kit::v4::packet::VariablePacketError),
    #[error(transparent)]
    V5VariablePacket(#[from] mqtt_codec_kit::v5::packet::VariablePacketError),
}
