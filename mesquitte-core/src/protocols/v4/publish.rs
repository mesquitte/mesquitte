use std::{cmp, sync::Arc};

use mqtt_codec_kit::{
    common::{
        qos::QoSWithPacketIdentifier, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX,
    },
    v4::packet::{
        DisconnectPacket, PubackPacket, PubcompPacket, PublishPacket, PubrecPacket, PubrelPacket,
        VariablePacket,
    },
};

use crate::{
    server::state::GlobalState,
    types::{outgoing::Outgoing, publish::PublishMessage, session::Session},
};

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

    let topic_name = packet.topic_name();
    if topic_name.is_empty() {
        log::debug!("Publish topic name cannot be empty");
        return (true, Some(DisconnectPacket::new().into()));
    }

    if topic_name.starts_with(SHARED_PREFIX)
        || topic_name.contains(MATCH_ALL_STR)
        || topic_name.contains(MATCH_ONE_STR)
    {
        log::debug!("invalid topic name: {:?}", topic_name);
        return (true, Some(DisconnectPacket::new().into()));
    }
    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        log::debug!("invalid duplicate flag in QoS 0 publish message");
        return (true, Some(DisconnectPacket::new().into()));
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
            (false, Some(PubackPacket::new(packet_id).into()))
        }
        QoSWithPacketIdentifier::Level2(packet_id) => {
            if !packet.dup() {
                session
                    .pending_packets()
                    .push_incoming(packet_id, packet.into());
            }
            (false, Some(PubrecPacket::new(packet_id).into()))
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
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        packet.topic_name(),
        packet.payload(),
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
        match content.topic_filter.as_ref() {
            Some(filter) => {
                for (client_id, subscribe_qos) in &content.clients {
                    senders.push((*client_id, filter.clone(), *subscribe_qos));
                }
            }
            None => log::warn!("topic filter is empty in content : {:?}", content),
        }
    }

    for (receiver_client_id, _, qos) in senders {
        if let Some(sender) = global.get_outgoing_sender(&receiver_client_id) {
            if sender.is_closed() {
                log::warn!(
                    "client#{:?} outgoing sender channel is closed",
                    global.get_client_identifier(&receiver_client_id)
                );
                continue;
            }
            if let Err(err) = sender
                .send(Outgoing::Publish(qos, Box::new(packet.clone())))
                .await
            {
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
        let inner = msg.message().to_owned();
        dispatch_publish(session, inner, global.clone()).await;
    }

    PubcompPacket::new(pid)
}

pub(super) fn receive_outgoing_publish(
    session: &mut Session,
    subscribe_qos: QualityOfService,
    message: PublishMessage,
) -> PublishPacket {
    log::debug!(
        r#"client#{} receive outgoing publish message:
topic name : {:?}
   payload : {:?}
     flags : publish qos={:?}, subscribe_qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        message.topic_name(),
        message.payload(),
        message.qos(),
        subscribe_qos,
        message.retain(),
        message.dup(),
    );

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

    let _matched = session.pending_packets().puback(pid);

    session.pending_packets().clean_outgoing();
}

pub(super) fn handle_pubrec(session: &mut Session, pid: u16) -> PubrelPacket {
    log::debug!(
        "client#{} received a pubrec packet, id : {}",
        session.client_identifier(),
        pid
    );

    let _matched = session.pending_packets().pubrec(pid);

    PubrelPacket::new(pid)
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

        packets.push(packet);
    }

    packets
}
