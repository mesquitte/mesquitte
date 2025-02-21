use std::{
    hash::{Hash, Hasher},
    io,
};

use foldhash::HashMap;
use mqtt_codec_kit::common::QualityOfService;
use parking_lot::RwLock;

use crate::{
    error,
    store::message::{MessageStore, PendingPublishMessage, PublishMessage, get_unix_ts},
};

#[derive(Debug)]
struct ReceivedMessage {
    message: PublishMessage,
    add_at: u64,
}

#[derive(Debug)]
struct PendingMessage {
    message: PendingPublishMessage,
    retrieve_attempts: usize,
    add_at: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MessageKey {
    packet_id: u16,
    qos: QualityOfService,
}

impl Hash for MessageKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.packet_id.hash(state);
        let qos_value = match self.qos {
            QualityOfService::Level0 => 0,
            QualityOfService::Level1 => 1,
            QualityOfService::Level2 => 2,
        };
        qos_value.hash(state);
    }
}

#[derive(Default)]
pub struct MessageMemoryStore {
    max_packets: usize,
    max_attempts: usize,
    max_timeout: usize,
    retrieve_factor: usize,
    received_message: RwLock<HashMap<String, HashMap<u16, ReceivedMessage>>>,
    pending_message: RwLock<HashMap<String, HashMap<MessageKey, PendingMessage>>>,
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
            received_message: Default::default(),
            pending_message: Default::default(),
        }
    }
}

impl MessageStore for MessageMemoryStore {
    async fn save_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PublishMessage,
    ) -> Result<bool, io::Error> {
        if let Some(messages) = self.received_message.read().get(client_id) {
            if messages.len() > self.max_packets {
                error!(
                    "drop received publish packet {:?}, store is full: {}",
                    message,
                    messages.len()
                );
                return Ok(true);
            }
        }

        let mut received_message_guard = self.received_message.write();
        let packets = received_message_guard
            .entry(client_id.to_string())
            .or_default();

        packets.insert(
            packet_id,
            ReceivedMessage {
                message,
                add_at: get_unix_ts(),
            },
        );
        Ok(false)
    }

    async fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<PublishMessage>, io::Error> {
        if let Some(packets) = self.received_message.write().get_mut(client_id) {
            let ret = packets.remove(&packet_id).map(|v| v.message);
            let max_timeout = self.max_timeout as u64;
            let now_ts = get_unix_ts();
            packets.retain(|_, v| now_ts < max_timeout + v.add_at);
            return Ok(ret);
        }
        Ok(None)
    }

    async fn save_pending_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PendingPublishMessage,
    ) -> Result<bool, io::Error> {
        if let Some(messages) = self.pending_message.read().get(client_id) {
            if messages.len() > self.max_packets {
                error!(
                    "drop pending publish packet {:?}, store is full: {}",
                    message,
                    messages.len()
                );
                return Ok(true);
            }
        }

        let mut pending_message_guard = self.pending_message.write();
        let packets = pending_message_guard
            .entry(client_id.to_string())
            .or_default();
        let (qos, _) = message.qos().split();

        packets.insert(
            MessageKey { packet_id, qos },
            PendingMessage {
                message,
                retrieve_attempts: 1,
                add_at: get_unix_ts(),
            },
        );

        Ok(false)
    }

    async fn try_get_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Option<Vec<(u16, PendingPublishMessage)>>, io::Error> {
        if let Some(packets) = self.pending_message.write().get_mut(client_id) {
            if packets.is_empty() {
                return Ok(None);
            }

            let now_ts = get_unix_ts();
            let retrieve_factor = self.retrieve_factor as u64;
            let useful_values = packets
                .iter_mut()
                .filter_map(|(key, msg)| {
                    if msg.retrieve_attempts > self.max_attempts {
                        return None;
                    }
                    if now_ts > retrieve_factor * msg.retrieve_attempts as u64 + msg.add_at {
                        msg.retrieve_attempts += 1;
                        msg.message.set_dup(true);
                        Some((key.packet_id, msg.message.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let max_timeout = self.max_timeout as u64;
            packets.retain(|_, msg| match msg.message.pubrec_at() {
                Some(pubrec_at) => now_ts < max_timeout + pubrec_at,
                None => now_ts < max_timeout + msg.add_at,
            });
            return Ok(Some(useful_values));
        }
        Ok(None)
    }

    async fn get_all_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Option<Vec<(u16, PendingPublishMessage)>>, io::Error> {
        if let Some(packets) = self.pending_message.write().get_mut(client_id) {
            if packets.is_empty() {
                return Ok(None);
            }

            let useful_values = packets
                .iter_mut()
                .filter_map(|(key, msg)| {
                    if msg.retrieve_attempts > self.max_attempts {
                        return None;
                    }

                    msg.retrieve_attempts += 1;
                    msg.message.set_dup(true);
                    Some((key.packet_id, msg.message.clone()))
                })
                .collect();

            let now_ts = get_unix_ts();
            let max_timeout = self.max_timeout as u64;

            packets.retain(|_, msg| match msg.message.pubrec_at() {
                Some(pubrec_at) => now_ts < max_timeout + pubrec_at,
                None => now_ts < max_timeout + msg.add_at,
            });
            return Ok(Some(useful_values));
        }
        Ok(None)
    }

    async fn puback(&self, client_id: &str, packet_id: u16) -> Result<bool, io::Error> {
        let key = MessageKey {
            packet_id,
            qos: QualityOfService::Level1,
        };
        match self.pending_message.write().get_mut(client_id) {
            Some(packets) => match packets.remove(&key) {
                Some(_) => Ok(true),
                None => Ok(false),
            },
            None => Ok(false),
        }
    }

    async fn pubrec(&self, client_id: &str, packet_id: u16) -> Result<bool, io::Error> {
        let key = MessageKey {
            packet_id,
            qos: QualityOfService::Level2,
        };
        if let Some(packets) = self.pending_message.write().get_mut(client_id) {
            return if let Some(pkt) = packets.get_mut(&key) {
                pkt.message.renew_pubrec_at();
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn pubcomp(&self, client_id: &str, packet_id: u16) -> Result<bool, io::Error> {
        let key = MessageKey {
            packet_id,
            qos: QualityOfService::Level2,
        };
        match self.pending_message.write().get_mut(client_id) {
            Some(packets) => match packets.remove(&key) {
                Some(_) => Ok(true),
                None => Ok(false),
            },
            None => Ok(false),
        }
    }

    async fn is_full(&self, client_id: &str) -> Result<bool, io::Error> {
        let l = match self.received_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        } + match self.pending_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(l > self.max_packets)
    }

    async fn message_count(&self, client_id: &str) -> Result<usize, io::Error> {
        let l = match self.received_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        } + match self.pending_message.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(l)
    }

    async fn clear_all(&self, client_id: &str) -> Result<(), io::Error> {
        self.pending_message.write().remove(client_id);
        self.received_message.write().remove(client_id);
        Ok(())
    }
}
