use std::{cmp, sync::Arc};

use mqtt_codec_kit::{
    common::{
        qos::QoSWithPacketIdentifier, QualityOfService, TopicFilter, MATCH_ALL_STR, MATCH_ONE_STR,
        SHARED_PREFIX,
    },
    v5::{
        control::{
            DisconnectReasonCode, PubackReasonCode, PubcompReasonCode, PubrecReasonCode,
            PubrelReasonCode,
        },
        packet::{
            PubackPacket, PubcompPacket, PublishPacket, PubrecPacket, PubrelPacket, VariablePacket,
        },
    },
};

use crate::{
    server::state::GlobalState,
    types::{outgoing::Outgoing, publish::PublishMessage, session::Session},
};

use super::common::build_error_disconnect;

pub(super) async fn handle_publish(
    session: &mut Session,
    packet: PublishPacket,
    global: Arc<GlobalState>,
) -> (bool, Option<VariablePacket>) {
    log::debug!(
        r#"client#{} received a publish packet:
topic name : {:?}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        packet.topic_name(),
        packet.payload(),
        packet.qos(),
        packet.retain(),
        packet.dup(),
    );

    if session.pending_packets().incoming_len() >= session.receive_maximum().into() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ReceiveMaximumExceeded,
            "received more than Receive Maximum publication",
        );
        return (true, Some(err_pkt.into()));
    }

    let topic_name = packet.topic_name();
    // TODO: topic alias and max topic alias
    if topic_name.is_empty() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "topic name cannot be empty",
        );
        return (true, Some(err_pkt.into()));
    }

    if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "topic name cannot start with '$' or contain '+' or '#'",
        );
        return (true, Some(err_pkt.into()));
    }
    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "invalid duplicate flag in QoS 0 publish message",
        );
        return (true, Some(err_pkt.into()));
    }

    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {
            dispatch_publish(session, packet.into(), global).await;
            (false, None)
        }
        QoSWithPacketIdentifier::Level1(packet_id) => {
            if !packet.dup() {
                dispatch_publish(session, packet.into(), global).await;
            }

            (
                false,
                Some(PubackPacket::new(packet_id, PubackReasonCode::Success).into()),
            )
        }
        QoSWithPacketIdentifier::Level2(packet_id) => {
            if !packet.dup() {
                session
                    .pending_packets()
                    .push_incoming(packet_id, packet.into());
            }
            (
                false,
                Some(PubrecPacket::new(packet_id, PubrecReasonCode::Success).into()),
            )
        }
    }
}

// Dispatch a publish message from client or will to matched clients
pub(super) async fn dispatch_publish(
    session: &mut Session,
    packet: PublishMessage,
    global: Arc<GlobalState>,
) {
    log::debug!(
        r#"client#{} dispatch publish message:
topic name : {:?}
   payload : {:?}
properties : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        packet.topic_name(),
        packet.payload(),
        packet.properties(),
        packet.qos(),
        packet.retain(),
        packet.dup(),
    );

    if packet.retain() {
        if packet.payload().is_empty() {
            global.retain_table().remove(packet.topic_name());
        } else {
            global
                .retain_table()
                .insert(Arc::new((session.client_identifier(), &packet).into()));
        }
    }

    let matches = global.route_table().get_matches(packet.topic_name());
    let mut senders = Vec::with_capacity(matches.len());
    for content in matches {
        let content = content.read();
        let subscribe_filter = content.topic_filter.as_ref().unwrap();
        match content.topic_filter.as_ref() {
            Some(filter) => {
                for (client_id, subscribe_qos) in &content.clients {
                    senders.push((*client_id, filter.clone(), *subscribe_qos));
                }
            }
            None => log::warn!("topic filter is empty in content : {:?}", content),
        }
        for (group_name, shared_clients) in &content.groups {
            // TODO: config: shared subscription available
            // TODO: config: shared subscription mode
            let (client_id, subscribe_qos) =
                shared_clients.get_by_hash(session.client_identifier());
            // TODO: optimize this alloc later
            let full_filter = TopicFilter::new(format!(
                "{SHARED_PREFIX}{group_name}/{:?}",
                subscribe_filter,
            ))
            .expect("full topic filter");
            senders.push((client_id, full_filter, subscribe_qos));
        }
    }

    for (receiver_client_id, _subscribe_filter, qos) in senders {
        if let Some(sender) = global.get_outgoing_sender(&receiver_client_id) {
            if sender.is_closed() {
                // TODO: client identifier
                log::warn!("{} offline", receiver_client_id);
                continue;
            }
            if let Err(err) = sender.send(Outgoing::Publish(qos, packet.clone())).await {
                log::error!("{} send publish message: {}", receiver_client_id, err,)
            }
        }
    }
}

pub(super) async fn handle_pubrel(
    session: &mut Session,
    global: Arc<GlobalState>,
    pid: u16,
) -> PubcompPacket {
    log::debug!(
        "client#{} received a pubrel packet, id : {}",
        session.client_identifier(),
        pid
    );

    session.pending_packets().clean_incoming();
    let mut start_idx = 0;
    while let Some((idx, msg)) = session
        .pending_packets()
        .get_unsent_incoming_packet(start_idx)
    {
        start_idx = idx + 1;
        let message = msg.message().to_owned();
        dispatch_publish(session, message, global.clone()).await;
    }

    PubcompPacket::new(pid, PubcompReasonCode::Success)
}

pub(super) fn receive_outgoing_publish(
    session: &mut Session,
    subscribe_qos: QualityOfService,
    // retain_as_published: bool,
    message: PublishMessage,
) -> PublishPacket {
    log::debug!(
        r#"client#{} receive outgoing publish message:
topic name : {:?}
   payload : {:?}
properties : {:?}
     flags : qos={:?}, subscribe_qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        message.topic_name(),
        message.payload(),
        message.properties(),
        message.qos(),
        subscribe_qos,
        message.retain(),
        message.dup(),
    );

    // let subscription_identifiers =
    //     if let Some(sub) = session.subscribes.get(message.subscribe_filter) {
    //         sub.id
    //     } else {
    //         // the client already unsubscribed.
    //         return None;
    //     };

    let properties = message.properties().cloned().unwrap_or_default();
    // TODO: add_subscription_identifiers?
    // for identifier in subscription_identifiers {
    //     properties.add_subscription_identifier(identifier);
    // }

    let final_qos = cmp::min(subscribe_qos, message.qos());
    let (packet_id, qos) = match final_qos {
        QualityOfService::Level0 => (None, QoSWithPacketIdentifier::Level0),
        QualityOfService::Level1 => {
            let packet_id = session.incr_server_packet_id();
            (Some(packet_id), QoSWithPacketIdentifier::Level1(packet_id))
        }
        QualityOfService::Level2 => {
            let packet_id = session.incr_server_packet_id();
            (Some(packet_id), QoSWithPacketIdentifier::Level2(packet_id))
        }
    };

    let mut packet = PublishPacket::new(message.topic_name().to_owned(), qos, message.payload());
    packet.set_dup(message.dup());
    packet.set_properties(properties);

    if let Some(packet_id) = packet_id {
        session
            .pending_packets()
            .push_outgoing(packet_id, subscribe_qos, message);
    }

    packet
}

pub(super) fn handle_puback(session: &mut Session, pid: u16) {
    log::debug!(
        "client#{} received a puback packet, id : {}",
        session.client_identifier(),
        pid
    );

    session.pending_packets().puback(pid);
    session.pending_packets().clean_outgoing();
}

pub(super) fn handle_pubrec(session: &mut Session, pid: u16) -> PubrelPacket {
    log::debug!(
        "client#{} received a pubrec packet, id : {}",
        session.client_identifier(),
        pid
    );

    if session.pending_packets().pubrec(pid) {
        PubrelPacket::new(pid, PubrelReasonCode::Success)
    } else {
        PubrelPacket::new(pid, PubrelReasonCode::PacketIdentifierNotFound)
    }
}

pub(super) fn handle_pubcomp(session: &mut Session, pid: u16) {
    log::debug!(
        "client#{} received a pubcomp packet, id : {}",
        session.client_identifier(),
        pid
    );

    session.pending_packets().pubcomp(pid);
    session.pending_packets().clean_outgoing();
}

pub(super) async fn handle_will(session: &mut Session, global: Arc<GlobalState>) {
    log::debug!(
        r#"client#{} handle last will:
client side disconnected : {}
server side disconnected : {}
               last will : {:?}"#,
        session.client_identifier(),
        session.client_disconnected(),
        session.server_disconnected(),
        session.last_will(),
    );

    if let Some(last_will) = session.take_last_will() {
        dispatch_publish(session, last_will.into(), global.clone()).await;
        session.clear_last_will();
    }
}

pub(crate) fn get_unsent_outgoing_packet(session: &mut Session) -> Vec<PublishPacket> {
    let mut packets = Vec::new();
    let mut start_idx = 0;
    while let Some((idx, msg)) = session
        .pending_packets()
        .get_unsent_outgoing_packet(start_idx)
    {
        start_idx = idx + 1;

        let qos = match msg.final_qos() {
            QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
            QualityOfService::Level1 => QoSWithPacketIdentifier::Level1(msg.packet_id()),
            QualityOfService::Level2 => QoSWithPacketIdentifier::Level2(msg.packet_id()),
        };
        let topic_name = msg.message().topic_name().to_owned();
        let mut packet = PublishPacket::new(topic_name, qos, msg.message().payload());
        packet.set_dup(msg.message().dup());
        if let Some(properties) = msg.message().properties() {
            packet.set_properties(properties.clone());
        }

        packets.push(packet);
    }

    packets
}
