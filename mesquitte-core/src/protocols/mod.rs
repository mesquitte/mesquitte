use foldhash::HashSet;
use mqtt_codec_kit::common::TopicFilter;

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

impl ProtocolSessionState {
    pub fn subscriptions(&self) -> HashSet<TopicFilter> {
        match self {
            #[cfg(feature = "v4")]
            ProtocolSessionState::V4(session_state) => session_state.subscriptions().clone(),
            #[cfg(feature = "v5")]
            ProtocolSessionState::V5(session_state) => {
                session_state.subscriptions().keys().cloned().collect()
            }
        }
    }
}
