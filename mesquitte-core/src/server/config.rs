use std::{net::SocketAddr, path::Path};

#[derive(Clone, Debug)]
pub struct ServerConfig<P: AsRef<Path>> {
    pub addr: SocketAddr,
    pub tls: Option<TlsConfig<P>>,
}

impl<P: AsRef<Path>> ServerConfig<P> {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig<P>>) -> Self {
        Self { addr, tls }
    }
}

#[derive(Clone, Debug)]
pub struct TlsConfig<P: AsRef<Path>> {
    pub ca_file: Option<P>,
    pub cert_file: P,
    pub key_file: P,
    pub fail_if_no_peer_cert: bool,
}
