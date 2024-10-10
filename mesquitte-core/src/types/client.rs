use super::session::SessionState;

pub enum AddClientReceipt {
    Present(SessionState),
    New,
}
