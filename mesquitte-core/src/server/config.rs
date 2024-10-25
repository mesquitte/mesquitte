use std::{net::SocketAddr, path::PathBuf};

use mqtt_codec_kit::common::ProtocolLevel;

use super::Error;

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub addr: SocketAddr,
    pub tls: Option<TlsConfig>,
    pub version: ProtocolLevel,
}

impl ServerConfig {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig>, version: &str) -> Result<Self, Error> {
        Ok(Self {
            addr,
            tls,
            version: version.parse::<u8>()?.try_into()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub ca_file: Option<PathBuf>,
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
    pub fail_if_no_peer_cert: bool,
}

impl TlsConfig {
    pub fn new(
        ca_file: Option<PathBuf>,
        cert_file: PathBuf,
        key_file: PathBuf,
        fail_if_no_peer_cert: bool,
    ) -> Self {
        Self {
            ca_file,
            cert_file,
            key_file,
            fail_if_no_peer_cert,
        }
    }
}
