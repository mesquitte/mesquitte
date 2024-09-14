use std::sync::Arc;

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR},
    v5::{
        control::{PubackReasonCode, PubcompReasonCode, PubrecReasonCode, PubrelReasonCode},
        packet::{
            PubackPacket, PubcompPacket, PublishPacket, PubrecPacket, PubrelPacket, VariablePacket,
        },
    },
};

use crate::{
    server::state::GlobalState,
    types::{error::Error, outgoing::Outgoing, publish::PublishMessage, session::Session},
};

pub(super) async fn handle_publish(
    session: &mut Session,
    packet: &PublishPacket,
    global: Arc<GlobalState>,
) -> Result<Option<VariablePacket>, Error> {
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
        log::debug!("invalid empty topic name");
        return Err(Error::InvalidPublishPacket(
            "invalid empty topic name".to_string(),
        ));
    }
    if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
        log::debug!("invalid topic name: {:?}", topic_name);
        return Err(Error::InvalidPublishPacket(format!(
            "invalid topic name: {:?}",
            topic_name
        )));
    }
    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        log::debug!("invalid duplicate flag in QoS 0 publish message");
        return Err(Error::InvalidPublishPacket(
            "invalid duplicate flag in QoS 0 publish message".to_string(),
        ));
    }

    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {
            dispatch_publish(session, packet, global).await;
            Ok(None)
        }
        QoSWithPacketIdentifier::Level1(pid) => {
            if !packet.dup() {
                dispatch_publish(session, packet, global).await;
            }
            Ok(Some(
                PubackPacket::new(pid, PubackReasonCode::Success).into(),
            ))
        }
        QoSWithPacketIdentifier::Level2(pid) => {
            if !packet.dup() {
                session.pending_packets().push_incoming(pid, packet.into());
            }
            Ok(Some(
                PubrecPacket::new(pid, PubrecReasonCode::Success).into(),
            ))
        }
    }
}

// Dispatch a publish message from client or will to matched clients
pub(super) async fn dispatch_publish(
    session: &mut Session,
    packet: &PublishPacket,
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
                .insert(Arc::new((session.client_identifier(), packet).into()));
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

    for (receiver_client_id, filter, qos) in senders {
        if let Some(sender) = global.get_outgoing_sender(&receiver_client_id) {
            if let Err(err) = sender.send(Outgoing::Publish(qos, packet.into())).await {
                log::error!(
                    "send publish message failed, inner client id#{} topic : {:?}, qos : {:?}, {}",
                    receiver_client_id,
                    filter,
                    qos,
                    err,
                )
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
    while let Some((idx, packet)) = session
        .pending_packets()
        .get_unsent_incoming_packet(start_idx)
    {
        let publish_packet = PublishPacket::new(
            packet.inner().topic_name().to_owned(),
            QoSWithPacketIdentifier::Level2(packet.pid()),
            packet.inner().payload(),
        );
        dispatch_publish(session, &publish_packet, global.clone()).await;
        start_idx = idx + 1;
    }

    PubcompPacket::new(pid, PubcompReasonCode::Success)
}

// outgoing

pub(super) fn deliver_publish(
    session: &mut Session,
    qos: QualityOfService,
    mut msg: PublishMessage,
) -> PublishPacket {
    log::debug!(
        r#"client#{} deliver publish message to self session client:
topic name : {:?}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_identifier(),
        msg.topic_name(),
        msg.payload(),
        qos,
        msg.retain(),
        msg.dup(),
    );

    match qos {
        QualityOfService::Level0 => {
            let mut packet = PublishPacket::new(
                msg.topic_name().to_owned(),
                QoSWithPacketIdentifier::Level0,
                msg.payload(),
            );
            packet.set_retain(msg.retain());
            packet
        }
        QualityOfService::Level1 => {
            msg.set_dup();
            msg.set_qos(qos);

            let topic_name = msg.topic_name().to_owned();

            let pid = session.incr_server_packet_id();
            session.pending_packets().push_outgoing(pid, msg.to_owned());

            let mut packet = PublishPacket::new(
                topic_name,
                QoSWithPacketIdentifier::Level1(pid),
                msg.payload(),
            );
            packet.set_retain(msg.retain());
            packet
        }
        QualityOfService::Level2 => {
            msg.set_dup();
            msg.set_qos(qos);

            let topic_name = msg.topic_name().to_owned();

            let pid = session.incr_server_packet_id();
            session.pending_packets().push_outgoing(pid, msg.to_owned());

            let mut packet = PublishPacket::new(
                topic_name,
                QoSWithPacketIdentifier::Level2(pid),
                msg.payload(),
            );
            packet.set_retain(msg.retain());
            packet
        }
    }
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
server side disconnected : {}"#,
        session.client_identifier(),
        session.client_disconnected(),
        session.server_disconnected(),
    );

    if let Some(last_will) = session.take_last_will() {
        let qos = match last_will.qos() {
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
        let mut packet =
            PublishPacket::new(last_will.topic_name().to_owned(), qos, last_will.message());
        packet.set_retain(last_will.retain());
        dispatch_publish(session, &packet, global).await;
        session.clear_last_will();
    }
}
