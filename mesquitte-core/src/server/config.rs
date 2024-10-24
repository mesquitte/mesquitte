use std::{net::SocketAddr, path::Path};

use mqtt_codec_kit::common::ProtocolLevel;

use super::Error;

#[derive(Clone, Debug)]
pub struct ServerConfig<P: AsRef<Path>> {
    pub addr: SocketAddr,
    pub tls: Option<TlsConfig<P>>,
    pub version: ProtocolLevel,
}

impl<P: AsRef<Path>> ServerConfig<P> {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig<P>>, version: &str) -> Result<Self, Error> {
        Ok(Self {
            addr,
            tls,
            version: version.parse::<u8>()?.try_into()?,
        })
    }
}

#[derive(Clone, Debug)]
pub struct TlsConfig<P: AsRef<Path>> {
    pub ca_file: Option<P>,
    pub cert_file: P,
    pub key_file: P,
    pub fail_if_no_peer_cert: bool,
}
