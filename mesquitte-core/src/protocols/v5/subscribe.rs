use std::{collections::VecDeque, sync::Arc};

use mqtt_codec_kit::{
    common::QualityOfService,
    v5::{
        control::DisconnectReasonCode,
        packet::{
            suback::SubscribeReasonCode, SubackPacket, SubscribePacket, UnsubackPacket,
            UnsubscribePacket, VariablePacket,
        },
    },
};

use crate::{
    protocols::v5::{common::build_error_disconnect, publish::receive_outgoing_publish},
    server::state::GlobalState,
    types::session::Session,
};

use super::common::WritePacket;

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

    let properties = packet.properties();
    if properties.identifier().map(|id| id) == Some(0) {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "Subscription identifier value=0 is not allowed",
        );
        return WritePacket::Disconnect(err_pkt.into());
    }

    // TODO: config subscription identifier available false
    // properties.identifier().is_some() && !config.subscription_id_available()

    let mut reason_codes = Vec::with_capacity(packet.subscribes().len());
    let mut retain_packets: Vec<VariablePacket> = Vec::new();
    for (filter, subscribe_opts) in packet.subscribes() {
        // TODO: shared subscribe
        // SubscribeReasonCode::SharedSubscriptionNotSupported
        // SubscribeReasonCode::WildcardSubscriptionsNotSupported topic contain +/#

        let granted_qos = subscribe_opts.qos().to_owned();
        // TODO: granted max qos from config
        session.set_subscribe(filter.clone(), granted_qos);
        global.subscribe(filter, session.client_id(), granted_qos);

        for msg in global.retain_table().get_matches(filter) {
            let mut packet = receive_outgoing_publish(session, granted_qos, msg.into());
            packet.set_retain(true);

            retain_packets.push(packet.into());
        }
        let reason_code = match granted_qos {
            QualityOfService::Level0 => SubscribeReasonCode::GrantedQos0,
            QualityOfService::Level1 => SubscribeReasonCode::GrantedQos1,
            QualityOfService::Level2 => SubscribeReasonCode::GrantedQos2,
        };

        reason_codes.push(reason_code);
    }
    let mut queue: VecDeque<VariablePacket> = VecDeque::from(retain_packets);
    let suback_packet = SubackPacket::new(packet.packet_identifier(), reason_codes);
    // TODO: user properties
    queue.push_front(suback_packet.into());
    WritePacket::Packets(queue.into())
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

    let reason_codes = Vec::new();
    for filter in packet.subscribes() {
        global.unsubscribe(&filter, session.client_id());
        session.rm_subscribe(&filter);
    }

    UnsubackPacket::new(packet.packet_identifier(), reason_codes)
}
