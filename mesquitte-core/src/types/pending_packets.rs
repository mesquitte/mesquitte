use std::{cmp, collections::VecDeque, time::SystemTime};

use mqtt_codec_kit::common::QualityOfService;

use super::publish::PublishMessage;

pub struct OutgoingPublishPacket {
    packet_id: u16,
    subscribe_qos: QualityOfService,
    message: PublishMessage,
    added_at: u64,
    pubrec_at: Option<u64>,
    pubcomp_at: Option<u64>,
}

impl OutgoingPublishPacket {
    fn new(packet_id: u16, subscribe_qos: QualityOfService, message: PublishMessage) -> Self {
        Self {
            packet_id,
            message,
            subscribe_qos,
            added_at: get_unix_ts(),
            pubrec_at: None,
            pubcomp_at: None,
        }
    }

    pub fn packet_id(&self) -> u16 {
        self.packet_id
    }

    pub fn subscribe_qos(&self) -> QualityOfService {
        self.subscribe_qos
    }

    pub fn final_qos(&self) -> QualityOfService {
        cmp::min(self.subscribe_qos, self.message.qos())
    }

    pub fn message(&self) -> &PublishMessage {
        &self.message
    }
}

pub struct IncomingPublishPacket {
    message: PublishMessage,
    packet_id: u16,
    receive_at: u64,
    deliver_at: Option<u64>,
}

impl IncomingPublishPacket {
    fn new(packet_id: u16, message: PublishMessage) -> Self {
        Self {
            message,
            packet_id,
            receive_at: get_unix_ts(),
            deliver_at: None,
        }
    }

    pub fn message(&self) -> &PublishMessage {
        &self.message
    }

    pub fn packet_id(&self) -> u16 {
        self.packet_id
    }
}

pub struct PendingPackets {
    max_inflight: u16,
    max_packets: usize,
    // The ack packet timeout, when reached resent the packet
    timeout: u64,
    incoming_packets: VecDeque<IncomingPublishPacket>,
    outgoing_packets: VecDeque<OutgoingPublishPacket>,
}

impl PendingPackets {
    pub fn new(max_inflight: u16, max_packets: usize, timeout: u64) -> Self {
        Self {
            max_inflight,
            max_packets,
            timeout,
            incoming_packets: VecDeque::new(),
            outgoing_packets: VecDeque::new(),
        }
    }

    /// Push a packet into queue, return if the queue is full.
    pub fn push_incoming(&mut self, packet_id: u16, message: PublishMessage) -> bool {
        if self.incoming_packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many incoming packets in the queue: {}",
                message,
                self.incoming_packets.len()
            );
            return true;
        }

        self.incoming_packets
            .push_back(IncomingPublishPacket::new(packet_id, message));
        false
    }

    pub fn push_outgoing(
        &mut self,
        packet_id: u16,
        subscribe_qos: QualityOfService,
        message: PublishMessage,
    ) -> bool {
        if self.outgoing_packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many outgoing packets in the queue: {:?}",
                message,
                self.outgoing_packets.len()
            );
            return true;
        }

        self.outgoing_packets.push_back(OutgoingPublishPacket::new(
            packet_id,
            subscribe_qos,
            message,
        ));
        false
    }

    // pubrec outgoing
    pub fn pubrec(&mut self, target_pid: u16) -> bool {
        let current_inflight = cmp::min(self.max_inflight.into(), self.outgoing_packets.len());
        for idx in 0..current_inflight {
            let outgoing_packet = self.outgoing_packets.get_mut(idx).expect("pubrec packet");
            if outgoing_packet.packet_id.eq(&target_pid) {
                outgoing_packet.message.set_dup();
                outgoing_packet.pubrec_at = Some(get_unix_ts())
            }
        }
        false
    }

    fn release_outgoing(
        max_inflight: usize,
        target_pid: u16,
        qos: QualityOfService,
        packets: &mut VecDeque<OutgoingPublishPacket>,
    ) -> bool {
        let current_inflight = cmp::min(max_inflight, packets.len());
        for idx in 0..current_inflight {
            let outgoing_packet = packets.get_mut(idx).expect("release outgoing packet");
            if outgoing_packet.packet_id.eq(&target_pid) {
                match qos {
                    QualityOfService::Level1 => {
                        outgoing_packet.pubcomp_at = Some(get_unix_ts());
                        return true;
                    }
                    QualityOfService::Level2 => {
                        outgoing_packet.pubcomp_at = Some(get_unix_ts());
                        return true;
                    }
                    _ => {}
                }
            }
        }
        false
    }

    // puback QoS1 outgoing
    pub fn puback(&mut self, target_pid: u16) -> bool {
        Self::release_outgoing(
            self.max_inflight.into(),
            target_pid,
            QualityOfService::Level1,
            &mut self.outgoing_packets,
        )
    }

    // pubcomp QoS2 outgoing
    pub fn pubcomp(&mut self, target_pid: u16) -> bool {
        Self::release_outgoing(
            self.max_inflight.into(),
            target_pid,
            QualityOfService::Level2,
            &mut self.outgoing_packets,
        )
    }

    // shrink the queue to save memory
    pub fn shrink_queue<P>(queue: &mut VecDeque<P>) {
        if queue.capacity() >= 16 && queue.capacity() >= (queue.len() << 2) {
            queue.shrink_to(queue.len() << 1);
        } else if queue.is_empty() {
            queue.shrink_to(0);
        }
    }

    pub fn clean_incoming(&mut self) {
        let mut changed = false;
        let now_ts = get_unix_ts();
        let mut start_idx = 0;

        while let Some(packet) = self.incoming_packets.get(start_idx) {
            if packet.deliver_at.is_some() || now_ts >= self.timeout + packet.receive_at {
                self.incoming_packets.pop_front();
                changed = true;
            }
            start_idx += 1;
        }

        // shrink the queue to save memory
        if changed {
            Self::shrink_queue(&mut self.incoming_packets);
        }
    }

    pub fn clean_outgoing(&mut self) {
        let mut changed = false;
        let now_ts = get_unix_ts();
        let mut start_idx = 0;

        while let Some(packet) = self.outgoing_packets.get(start_idx) {
            start_idx += 1;
            if packet.pubcomp_at.is_some() {
                self.outgoing_packets.pop_front();
                changed = true;
                continue;
            }

            if QualityOfService::Level0.eq(&packet.message.qos()) {
                self.outgoing_packets.pop_front();
                changed = true;
                continue;
            }

            let last_packet_at = match packet.pubrec_at {
                Some(pubrec_at) => pubrec_at,
                None => packet.added_at,
            };

            if now_ts >= self.timeout + last_packet_at {
                self.outgoing_packets.pop_front();
                changed = true;
            }
        }

        // shrink the queue to save memory
        if changed {
            Self::shrink_queue(&mut self.outgoing_packets);
        }
    }

    pub fn get_unsent_incoming_packet(
        &mut self,
        start_idx: usize,
    ) -> Option<(usize, &IncomingPublishPacket)> {
        let now_ts = get_unix_ts();
        let current_inflight = cmp::min(self.max_inflight as usize, self.incoming_packets.len());
        let mut next_idx = None;
        for idx in start_idx..current_inflight {
            let packet = self.incoming_packets.get_mut(idx).expect("incoming packet");
            if packet.deliver_at.is_some() {
                continue;
            }

            if now_ts <= self.timeout + packet.receive_at {
                next_idx = Some(idx);
                packet.deliver_at = Some(get_unix_ts());
                break;
            }
        }
        next_idx.map(|idx| {
            (
                idx,
                self.incoming_packets.get(idx).expect("incoming packet"),
            )
        })
    }

    pub fn get_unsent_outgoing_packet(
        &mut self,
        start_idx: usize,
    ) -> Option<(usize, &OutgoingPublishPacket)> {
        let now_ts = get_unix_ts();
        let current_inflight = cmp::min(self.max_inflight as usize, self.outgoing_packets.len());
        let mut next_idx = None;
        for idx in start_idx..current_inflight {
            let packet = self.outgoing_packets.get_mut(idx).expect("outgoing packet");
            if packet.pubcomp_at.is_some() {
                continue;
            }

            let last_packet_at = match packet.pubrec_at {
                Some(pubrec_at) => pubrec_at,
                None => packet.added_at,
            };

            if now_ts <= self.timeout + last_packet_at {
                next_idx = Some(idx);
                packet.message.set_dup();
                break;
            }
        }

        next_idx.map(|idx| {
            (
                idx,
                self.outgoing_packets.get(idx).expect("outgoing packet"),
            )
        })
    }

    pub fn incoming_len(&self) -> usize {
        self.incoming_packets.len()
    }

    pub fn outgoing_len(&self) -> usize {
        self.outgoing_packets.len()
    }

    pub fn set_max_inflight(&mut self, new_value: u16) {
        self.max_inflight = new_value;
    }
}

/// Unix timestamp as seconds
pub fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
