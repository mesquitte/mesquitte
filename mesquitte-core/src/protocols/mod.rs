use std::io;

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
    #[error("client disconnected.")]
    Disconnect,
    #[error("server disconnected.")]
    ServerDisconnect,
    #[error("Invalid Topic : {0}")]
    Topic(String),
    #[error("New Client : {0} ")]
    DupClient(String),
    #[error("Kick Client : {0} ")]
    Kick(String),
    #[error("Empty subscribes. ")]
    EmptySubscribes,
}

pub enum ProtocolSessionState {
    #[cfg(feature = "v4")]
    V4(v4::session::SessionState),
    #[cfg(feature = "v5")]
    V5(v5::session::SessionState),
}
