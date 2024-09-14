use std::fmt::Display;

use mqtt_codec_kit::common::QualityOfService;
use tokio::sync::mpsc;

use super::{publish::PublishMessage, session::SessionState};

#[derive(PartialEq)]
pub enum KickReason {
    FromAdmin,
    Expired,
}

impl Display for KickReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KickReason::FromAdmin => write!(f, "kicked by admin"),
            KickReason::Expired => write!(f, "session expired"),
        }
    }
}

pub enum Outgoing {
    // Publish(${subscribe_qos}, ${publish_msg})
    Publish(QualityOfService, PublishMessage),
    Online(mpsc::Sender<SessionState>),
    Kick(KickReason),
}
