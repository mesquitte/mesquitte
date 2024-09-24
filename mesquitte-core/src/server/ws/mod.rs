mod ws_stream;

pub mod server;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("tungstenite Error : {0}")]
    Accept(#[from] tungstenite::Error),
    #[error("Missing tls config")]
    MissingTlsConfig,
    #[error("Wrong tls config: {0}")]
    Rustls(#[from] crate::server::rustls::Error),
}
