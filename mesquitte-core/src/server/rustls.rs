use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use rustls::{server::WebPkiClientVerifier, RootCertStore};
use tokio_rustls::{
    rustls::{Error as RustlsError, ServerConfig},
    TlsAcceptor,
};

use super::config::TlsConfig;

#[derive(Debug, thiserror::Error)]
#[error("Acceptor error")]
pub enum Error {
    #[error("I/O {0}")]
    Io(#[from] std::io::Error),
    #[error("Rustls error {0}")]
    Rustls(#[from] RustlsError),
    #[error("Invalid CA cert file {0}")]
    InvalidCACert(String),
    #[error("Invalid server key file {0}")]
    InvalidServerKey(String),
}

pub fn rustls_server_config<P: AsRef<Path>>(cfg: &TlsConfig<P>) -> Result<ServerConfig, Error> {
    let cert_file = &mut BufReader::new(File::open(cfg.cert_file.as_ref())?);
    let key_file = &mut BufReader::new(File::open(cfg.key_file.as_ref())?);

    let cert_chain = rustls_pemfile::certs(cert_file).collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(key_file)?
        .ok_or(Error::InvalidServerKey("invalid server key".to_string()))?;

    let client_auth = if cfg.fail_if_no_peer_cert {
        match &cfg.ca_file {
            Some(ca) => {
                let ca_file = &mut BufReader::new(File::open(ca)?);
                let cert_chain = rustls_pemfile::certs(ca_file).collect::<Result<Vec<_>, _>>()?;
                let mut client_auth_roots = RootCertStore::empty();
                for root in cert_chain {
                    client_auth_roots
                        .add(root)
                        .map_err(|e| Error::InvalidCACert(e.to_string()))?;
                }
                WebPkiClientVerifier::builder(client_auth_roots.into())
                    .build()
                    .map_err(|e| Error::InvalidCACert(e.to_string()))?
            }
            None => return Err(Error::InvalidCACert("empty ca".to_string())),
        }
    } else {
        WebPkiClientVerifier::no_client_auth()
    };

    ServerConfig::builder()
        .with_client_cert_verifier(client_auth)
        .with_single_cert(cert_chain, key)
        .map_err(|e| Error::InvalidCACert(e.to_string()))
}

pub fn rustls_acceptor<P: AsRef<Path>>(cfg: &TlsConfig<P>) -> Result<TlsAcceptor, Error> {
    Ok(TlsAcceptor::from(Arc::new(rustls_server_config(cfg)?)))
}
