use mqtt_codec_kit::common::TopicNameError;
use tokio::sync::mpsc::error::SendError;

use super::{outgoing::Outgoing, session::SessionState};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid topic name: {0}")]
    InvalidTopicName(#[from] TopicNameError),
    #[error(transparent)]
    V4VariablePacket(#[from] mqtt_codec_kit::v4::packet::VariablePacketError),
    #[error(transparent)]
    V5VariablePacket(#[from] mqtt_codec_kit::v5::packet::VariablePacketError),
    #[error("invalid connect packet")]
    InvalidConnectPacket,
    #[error("invalid publish packet: {0}")]
    InvalidPublishPacket(String),
    #[error(transparent)]
    SendSessionState(#[from] SendError<SessionState>),
    #[error(transparent)]
    SendOutgoing(#[from] SendError<Outgoing>),
    #[error("empty session state")]
    EmptySessionState,
    #[error("unsupported Packet")]
    UnsupportedPacket,
}
