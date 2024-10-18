use std::{fmt::Display, time::Duration};

use dashmap::DashMap;
use mqtt_codec_kit::common::QualityOfService;
use tokio::{
    sync::mpsc::{self, channel},
    time,
};

use crate::store::message::IncomingPublishMessage;

pub enum AddClientReceipt {
    Present(u16),
    New,
}

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

pub enum DispatchMessage {
    Publish(QualityOfService, Box<IncomingPublishMessage>),
    Online(mpsc::Sender<u16>),
    Kick(KickReason),
}

#[derive(Default)]
pub struct GlobalState {
    // TODO: metrics?
    // TODO: config content
    // max qos
    // max connection ?
    // read channel size
    // outgoing channel size
    // max packet size-> v3?
    // max inflight size
    // max inflight message size
    // retain table enable
    // max retain table size?

    // v5
    // max client packet size
    // max topic alias
    // max keep alive
    // min keep alive
    // config: Arc<Config>,
    clients: DashMap<String, mpsc::Sender<DispatchMessage>, foldhash::fast::RandomState>,
}

impl GlobalState {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub async fn add_client(
        &self,
        client_id: &str,
        new_sender: mpsc::Sender<DispatchMessage>,
    ) -> AddClientReceipt {
        if let Some(old_sender) = self.get_outgoing_sender(client_id) {
            if !old_sender.is_closed() {
                let (control_sender, mut control_receiver) = channel(1);
                match old_sender
                    .send(DispatchMessage::Online(control_sender))
                    .await
                {
                    Ok(()) => {
                        // TODO: config: build session state timeout
                        match time::timeout(Duration::from_secs(10), control_receiver.recv()).await
                        {
                            Ok(data) => {
                                if let Some(state) = data {
                                    self.clients.insert(client_id.to_owned(), new_sender);
                                    return AddClientReceipt::Present(state);
                                }
                            }
                            Err(_) => {
                                log::warn!("receive old session state timeout");
                            }
                        }
                    }
                    Err(err) => {
                        log::warn!("send online message to old session: {err}")
                    }
                }
            }
        }

        self.clients.insert(client_id.to_owned(), new_sender);
        AddClientReceipt::New
    }

    pub fn remove_client(&self, client_id: &str) {
        self.clients.remove(client_id);
    }

    pub fn get_outgoing_sender(&self, client_id: &str) -> Option<mpsc::Sender<DispatchMessage>> {
        self.clients.get(client_id).map(|s| s.value().clone())
    }
}
