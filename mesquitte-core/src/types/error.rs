#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid connect packet")]
    InvalidConnectPacket,
}
