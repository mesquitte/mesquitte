use std::fmt;

use super::session::SessionState;

#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct ClientId(u64);

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "client#{}", self.0)
    }
}

impl From<u64> for ClientId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self(u64::MAX)
    }
}

impl ClientId {
    pub fn new(value: u64) -> ClientId {
        ClientId(value)
    }

    pub fn inner(&self) -> u64 {
        self.0
    }
}

pub enum AddClientReceipt {
    Present(ClientId, SessionState),
    New(ClientId),
}
