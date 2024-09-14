use std::{collections::VecDeque, sync::Arc};

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService},
    v4::packet::{
        suback::SubscribeReturnCode, PublishPacket, SubackPacket, SubscribePacket, UnsubackPacket,
        UnsubscribePacket, VariablePacket,
    },
};

use crate::{server::state::GlobalState, types::session::Session};

pub(super) fn handle_subscribe(
    session: &mut Session,
    packet: &SubscribePacket,
    global: &Arc<GlobalState>,
) -> Vec<VariablePacket> {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    let mut retain_packets: Vec<VariablePacket> = Vec::new();
    for (filter, granted_qos) in packet.subscribes() {
        if filter.is_shared() {
            log::warn!("mqtt v3.x don't support shared subscription");
            return_codes.push(SubscribeReturnCode::Failure);
            continue;
        }

        // TODO: granted max qos from config
        session.set_subscribe(filter.clone(), granted_qos.to_owned());
        global.subscribe(filter, session.client_id(), granted_qos.to_owned());

        for msg in global.retain_table().get_matches(filter) {
            let qos = match granted_qos {
                QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                QualityOfService::Level1 => {
                    let pid = session.incr_server_packet_id();
                    QoSWithPacketIdentifier::Level1(pid)
                }
                QualityOfService::Level2 => {
                    let pid = session.incr_server_packet_id();
                    QoSWithPacketIdentifier::Level2(pid)
                }
            };
            let mut payload = vec![0u8; msg.payload().len()];
            payload.copy_from_slice(msg.payload());

            let mut publish_packet = PublishPacket::new(msg.topic_name().to_owned(), qos, payload);
            publish_packet.set_retain(true);

            retain_packets.push(publish_packet.into());
        }

        return_codes.push((*granted_qos).into());
    }
    let mut queue: VecDeque<VariablePacket> = VecDeque::from(retain_packets);
    queue.push_front(SubackPacket::new(packet.packet_identifier(), return_codes).into());
    queue.into()
}

pub(super) fn handle_unsubscribe(
    session: &mut Session,
    packet: &UnsubscribePacket,
    global: &Arc<GlobalState>,
) -> UnsubackPacket {
    log::debug!(
        r#"{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_identifier(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    for filter in packet.subscribes() {
        global.unsubscribe(filter, session.client_id());
        session.rm_subscribe(filter);
    }

    UnsubackPacket::new(packet.packet_identifier())
}
