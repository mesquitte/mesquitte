use std::io;

use foldhash::HashMap;
use parking_lot::RwLock;

use crate::store::message::{
    get_unix_ts, MessageStore, PendingPublishMessage, ReceivedPublishMessage,
};

pub struct MessageMemoryStore {
    max_packets: usize,
    timeout: u64,
    received_publish_message: RwLock<HashMap<String, HashMap<u16, ReceivedPublishMessage>>>,
    pending_publish_message: RwLock<HashMap<String, HashMap<u16, PendingPublishMessage>>>,
}

impl MessageMemoryStore {
    pub fn new(max_packets: usize, timeout: u64) -> Self {
        Self {
            max_packets,
            timeout,
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
                log::error!(
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
        packet_id: u16,
        message: PendingPublishMessage,
    ) -> Result<bool, io::Error> {
        if let Some(queue) = self.pending_publish_message.read().get(client_id) {
            if queue.len() > self.max_packets {
                log::error!(
                    "drop pending publish packet {:?}, queue is full: {}",
                    message,
                    queue.len()
                );
                return Ok(true);
            }
        }

        let mut queue = self.pending_publish_message.write();
        let packets = queue.entry(client_id.to_string()).or_default();

        packets.insert(packet_id, message);

        Ok(false)
    }

    async fn retrieve_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Vec<(u16, PendingPublishMessage)>, io::Error> {
        let mut pending_packets = Vec::new();
        if let Some(packets) = self.pending_publish_message.read().get(client_id) {
            for (server_packet_id, pkt) in packets {
                pending_packets.push((*server_packet_id, pkt.clone()));
            }
        }
        Ok(pending_packets)
    }

    async fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<ReceivedPublishMessage>, io::Error> {
        if let Some(packets) = self.received_publish_message.write().get_mut(client_id) {
            return Ok(packets.remove(&packet_id));
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
                pkt.renew_pubrec_at();
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

    async fn clean_received_messages(&self, client_id: &str) -> Result<(), io::Error> {
        if let Some(packets) = self.received_publish_message.write().get_mut(client_id) {
            let now_ts = get_unix_ts();
            packets.retain(|_, v| now_ts < self.timeout + v.receive_at());
        }
        Ok(())
    }

    async fn clean_pending_messages(&self, client_id: &str) -> Result<(), io::Error> {
        if let Some(packets) = self.pending_publish_message.write().get_mut(client_id) {
            let now_ts = get_unix_ts();

            packets.retain(|_, v| match v.pubrec_at() {
                Some(pubrec_at) => now_ts < self.timeout + pubrec_at,
                None => now_ts < self.timeout + v.receive_at(),
            });
        }
        Ok(())
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
