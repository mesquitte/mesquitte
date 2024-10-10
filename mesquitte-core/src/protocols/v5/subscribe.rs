use std::{collections::VecDeque, sync::Arc};

use mqtt_codec_kit::{
    common::QualityOfService,
    v5::{
        control::DisconnectReasonCode,
        packet::{
            suback::SubscribeReasonCode, subscribe::RetainHandling, DisconnectPacket, SubackPacket,
            SubscribePacket, UnsubackPacket, UnsubscribePacket, VariablePacket,
        },
    },
};

use crate::{server::state::GlobalState, types::session::Session};

use super::{common::build_error_disconnect, publish::receive_outgoing_publish};

pub(super) fn handle_subscribe(
    session: &mut Session,
    packet: SubscribePacket,
    global: Arc<GlobalState>,
) -> Result<Vec<VariablePacket>, DisconnectPacket> {
    log::debug!(
        r#"{} received a subscribe packet:
 packet id : {}
    topics : {:?}
properties : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
        packet.properties(),
    );

    let properties = packet.properties();
    if properties.identifier() == Some(0) {
        let disconnect_packet = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "Subscription identifier value=0 is not allowed",
        );
        return Err(disconnect_packet);
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
        let exist = session.subscribe(filter.clone());
        global.subscribe(filter, session.client_id(), granted_qos);

        // TODO: config: retain available?
        let send_retain = !filter.is_shared()
            && match subscribe_opts.retain_handling {
                RetainHandling::SendAtSubscribe => true,
                RetainHandling::SendAtSubscribeIfNotExist => exist,
                RetainHandling::DoNotSend => false,
            };

        if send_retain {
            for msg in global.retain_table().get_matches(filter) {
                if subscribe_opts.no_local && msg.client_id().eq(session.client_id()) {
                    continue;
                }

                let mut packet = receive_outgoing_publish(session, granted_qos, msg.into());
                packet.set_retain(true);

                retain_packets.push(packet.into());
            }
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
        r#"client#{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );

    let reason_codes = Vec::new();
    for filter in packet.subscribes() {
        global.unsubscribe(filter, session.client_id());
        session.unsubscribe(filter);
    }

    UnsubackPacket::new(packet.packet_identifier(), reason_codes)
}
