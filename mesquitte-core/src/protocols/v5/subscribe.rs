use std::{collections::VecDeque, sync::Arc};

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService},
    v5::{
        control::DisconnectReasonCode,
        packet::{
            suback::SubscribeReasonCode, DisconnectPacket, PublishPacket, SubackPacket,
            SubscribePacket, UnsubackPacket, UnsubscribePacket, VariablePacket,
        },
    },
};

use crate::{
    protocols::v5::common::build_error_disconnect, server::state::GlobalState,
    types::session::Session,
};

pub(super) fn handle_subscribe(
    session: &mut Session,
    packet: &SubscribePacket,
    global: Arc<GlobalState>,
) -> Result<Vec<VariablePacket>, DisconnectPacket> {
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
        return Err(err_pkt);
    }

    // TODO: config subscription identifier available false
    // properties.identifier().is_some() && !config.subscription_id_available()

    let mut reason_codes = Vec::with_capacity(packet.subscribes().len());
    let mut retain_packets: Vec<VariablePacket> = Vec::new();
    for (filter, subscribe_opts) in packet.subscribes() {
        // TODO: shared subscribe
        // SubscribeReasonCode::SharedSubscriptionNotSupported
        // SubscribeReasonCode::WildcardSubscriptionsNotSupported topic contain +/#

        let granted_qos = subscribe_opts.qos();
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
            payload.copy_from_slice(&msg.payload());

            let mut publish_packet = PublishPacket::new(msg.topic_name().to_owned(), qos, payload);
            publish_packet.set_retain(true);
            if let Some(properties) = msg.properties() {
                publish_packet.set_properties(properties.to_owned());
            }

            retain_packets.push(publish_packet.into());
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
    Ok(queue.into())
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
