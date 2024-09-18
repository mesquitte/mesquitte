use std::fmt::Display;

use tokio::sync::mpsc::Sender;

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
    // Publish(${publish_msg})
    Publish(PublishMessage),
    Online(Sender<SessionState>),
    Kick(KickReason),
}
