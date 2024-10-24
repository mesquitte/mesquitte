use std::path::Path;

use mqtt_codec_kit::common::ProtocolLevel;

#[derive(Clone, Debug)]
pub struct ServerConfig<P: AsRef<Path>> {
    pub addr: String,
    pub tls: Option<TlsConfig<P>>,
    pub version: ProtocolLevel,
}

impl<P: AsRef<Path>> ServerConfig<P> {
    pub fn new(addr: String, tls: Option<TlsConfig<P>>, version: &str) -> Self {
        let v = version.parse::<u8>().unwrap();
        Self {
            addr,
            tls,
            version: ProtocolLevel::from_u8(v).unwrap(),
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
