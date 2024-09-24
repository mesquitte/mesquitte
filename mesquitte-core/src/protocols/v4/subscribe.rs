use std::sync::Arc;

use mqtt_codec_kit::v4::packet::{
    suback::SubscribeReturnCode, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket,
    VariablePacket,
};

use crate::{server::state::GlobalState, types::session::Session};

use super::{common::WritePacket, publish::receive_outgoing_publish};

pub(super) fn handle_subscribe(
    session: &mut Session,
    packet: &SubscribePacket,
    global: Arc<GlobalState>,
) -> WritePacket {
    log::debug!(
        r#"{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    let mut packets: Vec<VariablePacket> = Vec::new();
    for (filter, subscribe_qos) in packet.subscribes() {
        if filter.is_shared() {
            log::warn!("mqtt v3.x don't support shared subscription");
            return_codes.push(SubscribeReturnCode::Failure);
            continue;
        }

        // TODO: granted max qos from config
        let granted_qos = subscribe_qos.to_owned();
        session.set_subscribe(filter.clone(), granted_qos);
        global.subscribe(filter, session.client_id(), granted_qos);

        for msg in global.retain_table().get_matches(filter) {
            let mut packet = receive_outgoing_publish(session, granted_qos, msg.into());
            packet.set_retain(true);

            packets.push(packet.into());
        }

        return_codes.push(granted_qos.into());
    }
    packets.push(SubackPacket::new(packet.packet_identifier(), return_codes).into());
    WritePacket::Packets(packets)
}

pub(super) fn handle_unsubscribe(
    session: &mut Session,
    packet: &UnsubscribePacket,
    global: Arc<GlobalState>,
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
