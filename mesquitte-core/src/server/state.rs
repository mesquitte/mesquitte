use std::time::Duration;

use dashmap::DashMap;
use mqtt_codec_kit::common::{QualityOfService, TopicFilter};
use tokio::{
    sync::mpsc::{self, channel},
    time,
};

use crate::types::{
    client::AddClientReceipt, outgoing::Outgoing, retain_table::RetainTable,
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
    clients: DashMap<String, mpsc::Sender<Outgoing>, foldhash::fast::RandomState>,

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
        client_id: &str,
        new_sender: mpsc::Sender<Outgoing>,
    ) -> AddClientReceipt {
        if let Some(old_sender) = self.get_outgoing_sender(client_id) {
            if !old_sender.is_closed() {
                let (control_sender, mut control_receiver) = channel(1);
                match old_sender.send(Outgoing::Online(control_sender)).await {
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

    pub fn remove_client<'a>(
        &self,
        client_id: &str,
        subscribes: impl IntoIterator<Item = &'a TopicFilter>,
    ) {
        self.clients.remove(client_id);
        for filter in subscribes {
            self.route_table.unsubscribe(filter, client_id);
        }
    }

    pub fn subscribe(&self, filter: &TopicFilter, id: &str, qos: QualityOfService) {
        self.route_table.subscribe(filter, id, qos);
    }

    pub fn unsubscribe(&self, filter: &TopicFilter, id: &str) {
        self.route_table.unsubscribe(filter, id);
    }

    pub fn get_outgoing_sender(&self, client_id: &str) -> Option<mpsc::Sender<Outgoing>> {
        self.clients.get(client_id).map(|s| s.value().clone())
    }

    pub fn retain_table(&self) -> &RetainTable {
        &self.retain_table
    }

    pub fn route_table(&self) -> &RouteTable {
        &self.route_table
    }
}
