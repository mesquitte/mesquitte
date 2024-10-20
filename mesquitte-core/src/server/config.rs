use std::path::Path;

#[derive(Clone, Debug)]
pub struct ServerConfig<P: AsRef<Path>> {
    pub addr: String,
    pub tls: Option<TlsConfig<P>>,
    pub version: String,
}

impl<P: AsRef<Path>> ServerConfig<P> {
    pub fn new(addr: String, tls: Option<TlsConfig<P>>, version: &str) -> Self {
        Self {
            addr,
            tls,
            version: version.to_string(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TlsConfig<P: AsRef<Path>> {
    pub ca_file: Option<P>,
    pub cert_file: P,
    pub key_file: P,
    pub fail_if_no_peer_cert: bool,
}
