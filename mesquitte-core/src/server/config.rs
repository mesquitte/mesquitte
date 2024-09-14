use std::{net::SocketAddr, path::PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    pub tls: Option<TlsConfig>,
}

impl ServerConfig {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig>) -> Self {
        Self { addr, tls }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TlsConfig {
    pub ca_file: Option<PathBuf>,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub fail_if_no_peer_cert: bool,
}
