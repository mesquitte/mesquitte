use std::{collections::VecDeque, io, time::SystemTime};

use foldhash::HashMap;
use mqtt_codec_kit::common::QualityOfService;
use parking_lot::RwLock;

use crate::store::message::{IncomingPublishMessage, MessageStore, OutgoingPublishMessage};

pub fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[derive(Debug, Clone)]
pub struct IncomingPublishPacket {
    message: IncomingPublishMessage,
    packet_id: u16,
    receive_at: u64,
    pubrel_at: Option<u64>,
    deliver_at: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct OutgoingPublishPacket {
    message: OutgoingPublishMessage,
    added_at: u64,
    pubrec_at: Option<u64>,
    pubcomp_at: Option<u64>,
}

pub struct MessageMemoryStore {
    max_packets: usize,
    timeout: u64,
    incoming: RwLock<HashMap<String, VecDeque<IncomingPublishPacket>>>,
    outgoing: RwLock<HashMap<String, VecDeque<OutgoingPublishPacket>>>,
}

impl MessageMemoryStore {
    pub fn new(max_packets: usize, timeout: u64) -> Self {
        Self {
            max_packets,
            timeout,
            incoming: Default::default(),
            outgoing: Default::default(),
        }
    }

    fn shrink_queue<P>(queue: &mut VecDeque<P>) {
        let len = queue.len();
        if len == 0 {
            queue.shrink_to(0);
        } else if queue.capacity() > len * 4 {
            queue.shrink_to(len * 2);
        }
    }
}

impl MessageStore for MessageMemoryStore {
    async fn enqueue_incoming(
        &self,
        client_id: &str,
        packet_id: u16,
        message: IncomingPublishMessage,
    ) -> Result<bool, io::Error> {
        let incoming_guard = &self.incoming;
        let max_packets = self.max_packets;

        if let Some(queue) = incoming_guard.read().get(client_id) {
            if queue.len() > max_packets {
                log::error!(
                    "drop incoming packet {:?}, queue is full: {}",
                    message,
                    queue.len()
                );
                return Ok(true);
            }
        }

        let mut queue = incoming_guard.write();
        let packets = queue.entry(client_id.to_string()).or_default();

        packets.push_back(IncomingPublishPacket {
            message,
            packet_id,
            receive_at: get_unix_ts(),
            pubrel_at: None,
            deliver_at: None,
        });
        Ok(false)
    }

    async fn enqueue_outgoing(
        &self,
        client_id: &str,
        message: OutgoingPublishMessage,
    ) -> Result<bool, io::Error> {
        let outgoing_guard = &self.outgoing;
        let max_packets = self.max_packets;

        if let Some(queue) = outgoing_guard.read().get(client_id) {
            if queue.len() > max_packets {
                log::error!(
                    "drop outgoing packet {:?}, queue is full: {}",
                    message,
                    queue.len()
                );
                return Ok(true);
            }
        }

        let mut queue = outgoing_guard.write();
        let packets = queue.entry(client_id.to_string()).or_default();

        packets.push_back(OutgoingPublishPacket {
            message,
            added_at: get_unix_ts(),
            pubrec_at: None,
            pubcomp_at: None,
        });
        Ok(false)
    }

    async fn fetch_ready_incoming(
        &self,
        client_id: &str,
        max_inflight: usize,
    ) -> Result<Option<Vec<IncomingPublishMessage>>, io::Error> {
        let incoming_guard = &self.incoming;

        match incoming_guard.read().get(client_id) {
            Some(queue) => {
                let mut ret = Vec::new();
                for packet in queue {
                    if packet.pubrel_at.is_some() {
                        ret.push(packet.message.to_owned());
                    }
                    if ret.len() >= max_inflight {
                        return Ok(Some(ret));
                    }
                }

                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }

    async fn fetch_pending_outgoing(
        &self,
        client_id: &str,
    ) -> Result<Vec<OutgoingPublishMessage>, io::Error> {
        let outgoing_guard = &self.outgoing;

        let mut pending_packets = Vec::new();
        if let Some(packets) = outgoing_guard.read().get(client_id) {
            for packet in packets {
                if packet.pubcomp_at.is_none() {
                    pending_packets.push(packet.message.clone());
                }
            }
        }
        Ok(pending_packets)
    }

    async fn pubrel(&self, client_id: &str, packet_id: u16) -> Result<bool, io::Error> {
        let incoming_guard = &self.incoming;

        let mut queue = incoming_guard.write();

        if let Some(packets) = queue.get_mut(client_id) {
            return if let Some(pos) = packets
                .iter()
                .position(|packet| packet.packet_id == packet_id)
            {
                packets[pos].pubrel_at = Some(get_unix_ts());
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn puback(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        let outgoing_guard = &self.outgoing;

        let mut queue = outgoing_guard.write();

        if let Some(packets) = queue.get_mut(client_id) {
            return if let Some(pos) = packets.iter().position(|packet| {
                packet.message.server_packet_id() == server_packet_id
                    && packet.message.subscribe_qos() == QualityOfService::Level1
            }) {
                packets[pos].pubcomp_at = Some(get_unix_ts());
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn pubrec(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        let outgoing_guard = &self.outgoing;

        let mut queue = outgoing_guard.write();

        if let Some(packets) = queue.get_mut(client_id) {
            return if let Some(pos) = packets.iter().position(|packet| {
                packet.message.server_packet_id() == server_packet_id
                    && packet.message.subscribe_qos() == QualityOfService::Level2
                    && packet.pubrec_at.is_none()
            }) {
                packets[pos].pubrec_at = Some(get_unix_ts());
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn pubcomp(&self, client_id: &str, server_packet_id: u16) -> Result<bool, io::Error> {
        let outgoing_guard = &self.outgoing;

        let mut queue = outgoing_guard.write();

        if let Some(packets) = queue.get_mut(client_id) {
            return if let Some(pos) = packets.iter().position(|packet| {
                packet.message.server_packet_id() == server_packet_id
                    && packet.message.subscribe_qos() == QualityOfService::Level2
                    && packet.pubcomp_at.is_none()
            }) {
                packets[pos].pubcomp_at = Some(get_unix_ts());
                Ok(true)
            } else {
                Ok(false)
            };
        }

        Ok(false)
    }

    async fn purge_completed_incoming_messages(&self, client_id: &str) -> Result<(), io::Error> {
        let incoming_guard = &self.incoming;
        let timeout = self.timeout;

        let mut p = incoming_guard.write();
        if let Some(queue) = p.get_mut(client_id) {
            let now_ts = get_unix_ts();
            let original_len = queue.len();
            queue.retain(|packet| {
                !(packet.deliver_at.is_some() || now_ts >= timeout + packet.receive_at)
            });

            if queue.len() < original_len {
                Self::shrink_queue(queue);
            }
        }
        Ok(())
    }

    async fn purge_completed_outgoing_messages(&self, client_id: &str) -> Result<(), io::Error> {
        let outgoing_guard = &self.outgoing;

        let mut p = outgoing_guard.write();
        if let Some(queue) = p.get_mut(client_id) {
            let mut changed = false;
            let now_ts = get_unix_ts();
            if let Some(pos) = queue.iter().position(|packet| {
                packet.pubcomp_at.is_some() || now_ts >= self.timeout + packet.added_at
            }) {
                changed = true;
                queue.remove(pos);
            }

            if changed {
                Self::shrink_queue(queue);
            }
        }

        Ok(())
    }

    async fn is_full(&self, client_id: &str) -> Result<bool, io::Error> {
        let incoming_guard = &self.incoming;
        let outgoing_guard = &self.outgoing;

        let x = match incoming_guard.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };
        let y = match outgoing_guard.read().get(client_id) {
            Some(v) => v.len(),
            None => 0,
        };

        Ok(x + y > self.max_packets)
    }

    async fn remove_all(&self, client_id: &str) -> Result<(), io::Error> {
        self.outgoing.write().remove(client_id);
        self.incoming.write().remove(client_id);
        Ok(())
    }
}
