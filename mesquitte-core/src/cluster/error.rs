#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("No RPC Client established to {0} cause {1}")]
    NoAvailableRaftRPCClient(String, String),
}
