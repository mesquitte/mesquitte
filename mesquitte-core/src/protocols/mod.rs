#[cfg(feature = "v4")]
pub(crate) mod v4;
#[cfg(feature = "v5")]
pub(crate) mod v5;

pub enum ProtocolSessionState {
    #[cfg(feature = "v4")]
    V4(v4::session::SessionState),
    #[cfg(feature = "v5")]
    V5(v5::session::SessionState),
}
