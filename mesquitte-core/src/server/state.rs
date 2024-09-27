use std::{io, time::Duration};

use dashmap::DashMap;
use mqtt_codec_kit::common::{QualityOfService, TopicFilter};
use parking_lot::Mutex;
use tokio::{
    sync::mpsc::{self, channel},
    time,
};

use crate::types::{
    client_id::{AddClientReceipt, ClientId},
    error::Error,
    outgoing::Outgoing,
    retain_table::RetainTable,
    topic_router::RouteTable,
};

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
    // The next client internal id, use this mutex to keep `add_client` atomic
    // TODO: next_client_id overflow?
    next_client_id: Mutex<u64>,

    // client internal id => MQTT client identifier
    client_id_map: DashMap<ClientId, String, ahash::RandomState>,
    // MQTT client identifier => client internal id
    client_identifier_map: DashMap<String, ClientId, ahash::RandomState>,
    clients: DashMap<ClientId, mpsc::Sender<Outgoing>, ahash::RandomState>,

    route_table: RouteTable,
    retain_table: RetainTable,
}

impl GlobalState {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub async fn add_client(
        &self,
        client_identifier: &str,
        sender: mpsc::Sender<Outgoing>,
    ) -> Result<AddClientReceipt, Error> {
        let client_id_opt: Option<ClientId> = self
            .client_identifier_map
            .get(client_identifier)
            .map(|pair| *pair.value());

        if let Some(client_id) = client_id_opt {
            if let Some(old_sender) = self.get_outgoing_sender(&client_id) {
                if !old_sender.is_closed() {
                    let (control_sender, mut control_receiver) = channel(1);
                    old_sender
                        .send(Outgoing::Online(control_sender))
                        .await
                        .map_err(|err| {
                            log::warn!("send online message failed: {}", err);
                            io::Error::from(io::ErrorKind::InvalidData)
                        })?;

                    // TODO: config: build session state timeout
                    match time::timeout(Duration::from_secs(10), control_receiver.recv()).await {
                        Ok(data) => {
                            if let Some(state) = data {
                                self.clients.insert(client_id, sender);
                                return Ok(AddClientReceipt::Present(client_id, state));
                            }
                        }
                        Err(_) => {
                            log::warn!("receive old session state timeout");
                        }
                    }
                }
            }
        }

        let mut next_client_id = self.next_client_id.lock();

        let client_id = (*next_client_id).into();

        self.client_identifier_map
            .insert(client_identifier.to_string(), client_id);
        self.client_id_map
            .insert(client_id, client_identifier.to_string());
        self.clients.insert(client_id, sender);

        *next_client_id += 1;

        Ok(AddClientReceipt::New(client_id))
    }

    pub fn remove_client<'a>(
        &self,
        client_id: ClientId,
        subscribes: impl IntoIterator<Item = &'a TopicFilter>,
    ) {
        // keep client operation atomic
        let _guard = self.next_client_id.lock();

        if let Some((_, client_identifier)) = self.client_id_map.remove(&client_id) {
            self.client_identifier_map.remove(&client_identifier);
        }
        self.clients.remove(&client_id);
        for filter in subscribes {
            self.route_table.unsubscribe(filter, client_id);
        }
    }

    pub fn subscribe(&self, filter: &TopicFilter, id: ClientId, qos: QualityOfService) {
        self.route_table.subscribe(filter, id, qos);
    }

    pub fn unsubscribe(&self, filter: &TopicFilter, id: ClientId) {
        self.route_table.unsubscribe(filter, id);
    }

    pub fn get_outgoing_sender(&self, client_id: &ClientId) -> Option<mpsc::Sender<Outgoing>> {
        self.clients.get(client_id).map(|s| s.value().clone())
    }

    pub fn get_client_identifier(&self, client_id: &ClientId) -> Option<String> {
        self.client_id_map.get(client_id).map(|s| s.value().clone())
    }

    pub fn retain_table(&self) -> &RetainTable {
        &self.retain_table
    }

    pub fn route_table(&self) -> &RouteTable {
        &self.route_table
    }
}
