use std::io;

use foldhash::HashSet;
use mqtt_codec_kit::common::TopicFilter;

#[cfg(feature = "v4")]
pub(crate) mod v4;
#[cfg(feature = "v5")]
pub(crate) mod v5;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] io::Error),
    #[error("channel send error : {0}")]
    ChannelSend(#[from] kanal::SendError),
    #[cfg(feature = "v4")]
    #[error("Invalid Packet.")]
    V4InvalidPacket,
    #[error("client disconnected.")]
    Disconnect,
    #[error("Invalid Topic : {0}")]
    Topic(String),
    #[error("New Client : {0} ")]
    DupClient(String),
    #[error("Kick Client : {0} ")]
    Kick(String),
    #[error("Empty subscribes. ")]
    EmptySubscribes,
    #[cfg(feature = "v4")]
    #[error(transparent)]
    V4VariablePacket(#[from] mqtt_codec_kit::v4::packet::VariablePacketError),
    #[cfg(feature = "v5")]
    #[error(transparent)]
    V5VariablePacket(#[from] mqtt_codec_kit::v5::packet::VariablePacketError),
}

pub enum ProtocolSessionState {
    #[cfg(feature = "v4")]
    V4(v4::session::SessionState),
    #[cfg(feature = "v5")]
    V5(v5::session::SessionState),
}

impl ProtocolSessionState {
    pub fn subscriptions(&self) -> HashSet<TopicFilter> {
        match self {
            #[cfg(feature = "v4")]
            ProtocolSessionState::V4(session_state) => session_state.subscriptions().clone(),
            #[cfg(feature = "v5")]
            ProtocolSessionState::V5(session_state) => {
                session_state.subscriptions().keys().cloned().collect()
            }
        }
    }
}
