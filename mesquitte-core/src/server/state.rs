use std::{fmt::Display, time::Duration};

use dashmap::DashMap;
use kanal::{bounded_async, AsyncSender};
use mqtt_codec_kit::common::{QualityOfService, TopicFilter};
use tokio::time;

use crate::{protocols::ProtocolSessionState, store::message::PublishMessage, warn};

pub enum AddClientReceipt {
    Present(ProtocolSessionState),
    New,
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug)]
pub enum DeliverMessage {
    Publish(TopicFilter, QualityOfService, Box<PublishMessage>),
    Online(AsyncSender<ProtocolSessionState>),
    Kick(KickReason),
}

#[derive(Default)]
pub struct GlobalState {
    // TODO: metrics?
    // TODO: config content
    // max qos
    // max connection ?
    // read channel size
    // deliver channel size
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
    clients: DashMap<String, AsyncSender<DeliverMessage>, foldhash::fast::RandomState>,
}

impl GlobalState {
    pub async fn add_client(
        &self,
        client_id: &str,
        new_sender: AsyncSender<DeliverMessage>,
    ) -> AddClientReceipt {
        if let Some(old_sender) = self.get_deliver(client_id) {
            if !old_sender.is_closed() {
                // TODO: config: build session state timeout
                let receive_timeout = Duration::from_secs(10);
                let (control_sender, control_receiver) = bounded_async(1);
                let ret = old_sender
                    .send(DeliverMessage::Online(control_sender))
                    .await;
                match ret {
                    Ok(_) => match time::timeout(receive_timeout, control_receiver.recv()).await {
                        Ok(data) => match data {
                            Ok(state) => {
                                self.clients.insert(client_id.to_owned(), new_sender);
                                return AddClientReceipt::Present(state);
                            }
                            Err(err) => {
                                warn!("add client failed: {err}");
                            }
                        },
                        Err(_) => {
                            warn!("receive old session state timeout");
                        }
                    },
                    Err(err) => {
                        warn!("send online failed: {err}")
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

    pub fn get_deliver(&self, client_id: &str) -> Option<AsyncSender<DeliverMessage>> {
        self.clients.get(client_id).map(|s| s.value().clone())
    }
}
