use std::io;

use foldhash::HashMap;
use parking_lot::RwLock;

use crate::{
    error,
    store::message::{get_unix_ts, MessageStore, PendingPublishMessage, ReceivedPublishMessage},
};

pub struct PendingMessage {
    message: PendingPublishMessage,
    retrieve_attempts: usize,
}

pub struct MessageMemoryStore {
    max_packets: usize,
    max_attempts: usize,
    max_timeout: usize,
    retrieve_factor: usize,
    received_publish_message: RwLock<HashMap<String, HashMap<u16, ReceivedPublishMessage>>>,
    pending_publish_message: RwLock<HashMap<String, HashMap<u16, PendingMessage>>>,
}

impl MessageMemoryStore {
    pub fn new(max_packets: usize, max_timeout: usize, max_attempts: usize) -> Self {
        let retrieve_factor = (max_timeout * 2) / (max_attempts * (max_attempts + 1));
        let retrieve_factor = if retrieve_factor < 1 {
            1
        } else {
            retrieve_factor
        };

        Self {
            max_packets,
            max_attempts,
            max_timeout,
            retrieve_factor,
            received_publish_message: Default::default(),
            pending_publish_message: Default::default(),
        }
    }
}

impl MessageStore for MessageMemoryStore {
    async fn save_received_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: ReceivedPublishMessage,
    ) -> Result<bool, io::Error> {
        if let Some(queue) = self.received_publish_message.read().get(client_id) {
            if queue.len() > self.max_packets {
                error!(
                    "drop received publish packet {:?}, queue is full: {}",
                    message,
                    queue.len()
                );
                return Ok(true);
            }
        }

        let mut queue = self.received_publish_message.write();
        let packets = queue.entry(client_id.to_string()).or_default();
        packets.insert(packet_id, message);
        Ok(false)
    }

    async fn save_pending_message(
        &self,
        client_id: &str,
        message: PendingPublishMessage,
    ) -> Result<bool, io::Error> {
        if let Some(queue) = self.pending_publish_message.read().get(client_id) {
            if queue.len() > self.max_packets {
                error!(
                    "drop pending publish packet {:?}, queue is full: {}",
                    message,
                    queue.len()
                );
                return Ok(true);
            }
        }

        let mut queue = self.pending_publish_message.write();
        let packets = queue.entry(client_id.to_string()).or_default();

        packets.insert(
            message.server_packet_id(),
            PendingMessage {
                message,
                retrieve_attempts: 1,
            },
        );

        Ok(false)
    }

    async fn retrieve_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Option<Vec<PendingPublishMessage>>, io::Error> {
        if let Some(packets) = self.pending_publish_message.write().get_mut(client_id) {
            if packets.is_empty() {
                return Ok(None);
            }

            let now_ts = get_unix_ts();
            let retrieve_factor = self.retrieve_factor as u64;
            let max_timeout = self.max_timeout as u64;
            let useful_values = packets
                .iter_mut()
                .filter_map(|(_, msg)| {
                    if msg.retrieve_attempts > self.max_attempts {
                        return None;
                    }
                    if now_ts
                        > retrieve_factor * msg.retrieve_attempts as u64 + msg.message.receive_at()
                    {
                        msg.retrieve_attempts += 1;
                        Some(msg.message.clone())
                    } else {
                        None
                    }
                })
                .collect();

            packets.retain(|_, msg| match msg.message.pubrec_at() {
                Some(pubrec_at) => now_ts < max_timeout + pubrec_at,
                None => now_ts < max_timeout + msg.message.receive_at(),
            });
            return Ok(Some(useful_values));
        }
        Ok(None)
    }

    async fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<ReceivedPublishMessage>, io::Error> {
        if let Some(packets) = self.received_publish_message.write().get_mut(client_id) {
            let ret = packets.remove(&packet_id);
            let max_timeout = self.max_timeout as u64;

            let now_ts = get_unix_ts();
            packets.retain(|_, v| now_ts < max_timeout + v.receive_at());
            return Ok(ret);
        }
        Ok(None)
    }

    async fn puback(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        match self.pending_publish_message.write().get_mut(client_id) {
            Some(packets) => match packets.remove(&server_packet_id) {
                Some(_) => Ok(true),
                None => Ok(false),
            },
            None => Ok(false),
        }
    }

    async fn pubrec(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        if let Some(packets) = self.pending_publish_message.write().get_mut(client_id) {
            return if let Some(pkt) = packets.get_mut(&server_packet_id) {
                pkt.message.renew_pubrec_at();
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn pubcomp(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        match self.pending_publish_message.write().get_mut(client_id) {
            Some(packets) => match packets.remove(&server_packet_id) {
                Some(_) => Ok(true),
                None => Ok(false),
            },
            None => Ok(false),
        }
    }

    async fn is_full(&self, client_id: &str) -> Result<bool, io::Error> {
        let l = match self.received_publish_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        } + match self.pending_publish_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(l > self.max_packets)
    }

    async fn get_message_count(&self, client_id: &str) -> Result<usize, io::Error> {
        let l = match self.received_publish_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        } + match self.pending_publish_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(l)
    }

    async fn clear_all_messages(&self, client_id: &str) -> Result<(), io::Error> {
        self.pending_publish_message.write().remove(client_id);
        self.received_publish_message.write().remove(client_id);
        Ok(())
    }
}
