use std::fmt::Display;

use mqtt_codec_kit::common::QualityOfService;
use tokio::sync::mpsc::Sender;

use super::{publish::PublishMessage, session::SessionState};

#[derive(PartialEq)]
pub enum KickReason {
    FromAdmin,
}

impl Display for KickReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KickReason::FromAdmin => write!(f, "kicked by admin"),
        }
    }
}

pub enum Outgoing {
    Publish(QualityOfService, PublishMessage),
    Online(Sender<SessionState>),
    Kick(KickReason),
}
