use std::{cmp, collections::VecDeque, time::SystemTime};

use mqtt_codec_kit::common::QualityOfService;

use super::publish::PublishMessage;

pub struct OutgoingPacket {
    packet: PublishMessage,
    pid: u16,
    // add this packet timestamp as seconds
    added_at: u64,
    // pubrec this packet timestamp as seconds
    pubrec_at: Option<u64>,
    // pubcomp this packet timestamp as seconds
    pubcomp_at: Option<u64>,
    pubrec: bool,
    pubcomp: bool,
}

impl OutgoingPacket {
    fn new(pid: u16, packet: PublishMessage) -> Self {
        Self {
            added_at: get_unix_ts(),
            pubrec_at: None,
            pubcomp_at: None,
            pubrec: false,
            pubcomp: false,
            pid,
            packet,
        }
    }

    pub fn set_pubrec(&mut self) {
        self.pubrec = true;
        self.pubrec_at = Some(get_unix_ts())
    }

    pub fn set_pubcomp(&mut self) {
        self.pubcomp = true;
        self.pubcomp_at = Some(get_unix_ts())
    }
}

pub struct IncomingPacket {
    pid: u16,
    inner: PublishMessage,
    // receive this packet timestamp as seconds
    receive_at: u64,
    sent: bool,
}

impl IncomingPacket {
    fn new(pid: u16, packet: PublishMessage) -> Self {
        Self {
            receive_at: get_unix_ts(),
            pid,
            inner: packet,
            sent: false,
        }
    }

    pub fn inner(&self) -> &PublishMessage {
        &self.inner
    }

    pub fn pid(&self) -> u16 {
        self.pid
    }
}

pub struct PendingPackets {
    max_inflight: u16,
    max_packets: usize,
    // The ack packet timeout, when reached resent the packet
    timeout: u64,
    incoming_packets: VecDeque<IncomingPacket>,
    outgoing_packets: VecDeque<OutgoingPacket>,
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
    pub fn push_incoming(&mut self, pid: u16, packet: PublishMessage) -> bool {
        if self.incoming_packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many incoming packets in the queue: {}",
                packet,
                self.incoming_packets.len()
            );
            return true;
        }

        self.incoming_packets
            .push_back(IncomingPacket::new(pid, packet));
        false
    }

    pub fn push_outgoing(&mut self, pid: u16, packet: PublishMessage) -> bool {
        if self.outgoing_packets.len() >= self.max_packets {
            log::error!(
                "drop packet {:?}, due to too many outgoing packets in the queue: {}",
                packet,
                self.outgoing_packets.len()
            );
            return true;
        }

        self.outgoing_packets
            .push_back(OutgoingPacket::new(pid, packet));
        false
    }

    // pubrec outgoing
    pub fn pubrec(&mut self, target_pid: u16) -> bool {
        let current_inflight = cmp::min(self.max_inflight.into(), self.outgoing_packets.len());
        for idx in 0..current_inflight {
            let outgoing_packet = self.outgoing_packets.get_mut(idx).expect("pubrec packet");
            if outgoing_packet.pid.eq(&target_pid) {
                outgoing_packet.set_pubrec()
            }
        }
        false
    }

    fn release_outgoing(
        max_inflight: usize,
        target_pid: u16,
        qos: QualityOfService,
        packets: &mut VecDeque<OutgoingPacket>,
    ) -> bool {
        let current_inflight = cmp::min(max_inflight, packets.len());
        for idx in 0..current_inflight {
            let outgoing_packet = packets.get_mut(idx).expect("release outgoing packet");
            if outgoing_packet.pid.eq(&target_pid) {
                match qos {
                    QualityOfService::Level1 => {
                        outgoing_packet.set_pubcomp();
                        return true;
                    }
                    QualityOfService::Level2 => {
                        outgoing_packet.set_pubcomp();
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
            if packet.sent || now_ts >= self.timeout + packet.receive_at {
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
            if packet.pubcomp || now_ts >= self.timeout + packet.added_at {
                self.incoming_packets.pop_front();
                changed = true;
            }

            start_idx += 1;
        }

        // shrink the queue to save memory
        if changed {
            Self::shrink_queue(&mut self.outgoing_packets);
        }
    }

    pub fn get_unsent_incoming_packet(
        &mut self,
        start_idx: usize,
    ) -> Option<(usize, &IncomingPacket)> {
        let now_ts = get_unix_ts();
        let current_inflight = cmp::min(self.max_inflight as usize, self.incoming_packets.len());
        let mut next_idx = None;
        for idx in start_idx..current_inflight {
            let packet = self.incoming_packets.get_mut(idx).expect("incoming packet");
            if packet.sent {
                continue;
            }

            if now_ts <= self.timeout + packet.receive_at {
                next_idx = Some(idx);
                packet.sent = true;
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
}

/// Unix timestamp as seconds
pub fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
