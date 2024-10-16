pub mod server;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io Error : {0}")]
    Io(#[from] std::io::Error),
    #[error("Missing tls config")]
    MissingTlsConfig,
    #[cfg(feature = "rustls")]
    #[error("Wrong tls config: {0}")]
    Rustls(#[from] crate::server::rustls::Error),
}
