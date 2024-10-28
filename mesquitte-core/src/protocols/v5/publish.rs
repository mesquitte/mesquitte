use std::{cmp, io};

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR},
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
    debug, error,
    protocols::v5::common::build_error_disconnect,
    server::state::{DeliverMessage, GlobalState},
    store::{
        message::{MessageStore, PendingPublishMessage, ReceivedPublishMessage},
        retain::RetainMessageStore,
        topic::{RouteOption, TopicStore},
        Storage,
    },
    warn,
};

use super::session::Session;

pub(super) async fn handle_publish<'a, S>(
    session: &mut Session,
    packet: &PublishPacket,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<(bool, Option<VariablePacket>)>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} received a publish packet:
topic name : {:?}
   payload : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_id(),
        packet.topic_name(),
        packet.payload(),
        packet.qos(),
        packet.retain(),
        packet.dup(),
    );

    let message_count = storage.get_message_count(session.client_id()).await?;
    if message_count >= session.receive_maximum().into() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ReceiveMaximumExceeded,
            "received more than Receive Maximum publication",
        );
        return Ok((true, Some(err_pkt.into())));
    }

    let topic_name = packet.topic_name();
    // TODO: topic alias and max topic alias
    if topic_name.is_empty() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "topic name cannot be empty",
        );
        return Ok((true, Some(err_pkt.into())));
    }

    if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::TopicNameInvalid,
            "topic name cannot start with '$' or contain '+' or '#'",
        );
        return Ok((true, Some(err_pkt.into())));
    }

    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        let err_pkt = build_error_disconnect(
            session,
            DisconnectReasonCode::ProtocolError,
            "invalid duplicate flag in QoS 0 publish message",
        );
        return Ok((true, Some(err_pkt.into())));
    }

    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {
            deliver_publish_message(session, packet.into(), global, storage).await?;
            Ok((false, None))
        }
        QoSWithPacketIdentifier::Level1(packet_id) => {
            if !packet.dup() {
                deliver_publish_message(session, packet.into(), global, storage).await?;
            }
            Ok((
                false,
                Some(PubackPacket::new(packet_id, PubackReasonCode::Success).into()),
            ))
        }
        QoSWithPacketIdentifier::Level2(packet_id) => {
            if !packet.dup() {
                storage
                    .save_received_message(session.client_id(), packet_id, packet.into())
                    .await?;
            }
            Ok((
                false,
                Some(PubrecPacket::new(packet_id, PubrecReasonCode::Success).into()),
            ))
        }
    }
}

pub(super) async fn deliver_publish_message<'a, S>(
    session: &mut Session,
    packet: ReceivedPublishMessage,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} dispatch publish message:
topic name : {:?}
   payload : {:?}
properties : {:?}
     flags : qos={:?}, retain={}, dup={}"#,
        session.client_id(),
        packet.topic_name(),
        packet.payload(),
        packet.properties(),
        packet.qos(),
        packet.retain(),
        packet.dup(),
    );

    if packet.retain() {
        if packet.payload().is_empty() {
            storage.remove(packet.topic_name()).await?;
        } else {
            storage
                .insert((session.client_id(), &packet).into())
                .await?;
        }
    }

    let matches = TopicStore::search(storage.as_ref(), packet.topic_name()).await?;
    let mut senders = Vec::new();
    for (client_id, opt) in matches.normal_clients {
        match opt {
            RouteOption::V4(qos) => {
                senders.push((client_id.to_owned(), qos));
            }
            RouteOption::V5(subscribe_options) => {
                senders.push((client_id.to_owned(), subscribe_options.qos()));
            }
        }
    }

    // TODO: config: shared subscription available
    for (_group_name, shared_clients) in matches.shared_clients {
        // TODO: config: shared subscription mode
        // TODO: shared subscription index by group_name?
        for (client_id, opt) in shared_clients {
            match opt {
                RouteOption::V4(qos) => {
                    senders.push((client_id.to_owned(), qos));
                }
                RouteOption::V5(subscribe_options) => {
                    senders.push((client_id.to_owned(), subscribe_options.qos()));
                }
            }
            break;
        }
    }

    for (receiver_client_id, qos) in senders {
        if let Some(sender) = global.get_deliver(&receiver_client_id) {
            if sender.is_closed() {
                warn!("client#{:?} deliver channel is closed", receiver_client_id,);
                continue;
            }
            if let Err(err) = sender
                .send(DeliverMessage::Publish(qos, Box::new(packet.clone())))
                .await
            {
                error!("{} send publish: {}", receiver_client_id, err,)
            }
        }
    }

    Ok(())
}

pub(super) async fn handle_pubrel<'a, S>(
    session: &mut Session,
    packet_id: u16,
    storage: &'a Storage<S>,
) -> io::Result<PubcompPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a pubrel packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage.pubrel(session.client_id(), packet_id).await?;

    Ok(PubcompPacket::new(packet_id, PubcompReasonCode::Success))
}

pub(super) async fn handle_deliver_publish<'a, S>(
    session: &mut Session,
    subscribe_qos: QualityOfService,
    // retain_as_published: bool,
    message: &ReceivedPublishMessage,
    storage: &'a Storage<S>,
) -> io::Result<PublishPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} receive deliver publish message:
topic name : {:?}
   payload : {:?}
properties : {:?}
     flags : qos={:?}, subscribe_qos={:?}, retain={}, dup={}"#,
        session.client_id(),
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
        storage
            .save_pending_message(
                session.client_id(),
                PendingPublishMessage::new(packet_id, subscribe_qos, message.clone()),
            )
            .await?;
    }

    Ok(packet)
}

pub(super) async fn handle_puback<'a, S>(
    session: &mut Session,
    packet_id: u16,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a puback packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage.puback(session.client_id(), packet_id).await?;

    Ok(())
}

pub(super) async fn handle_pubrec<'a, S>(
    session: &mut Session,
    packet_id: u16,
    storage: &'a Storage<S>,
) -> io::Result<PubrelPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a pubrec packet, id : {}",
        session.client_id(),
        packet_id
    );

    let matched = storage.pubrec(session.client_id(), packet_id).await?;

    if matched {
        Ok(PubrelPacket::new(packet_id, PubrelReasonCode::Success))
    } else {
        Ok(PubrelPacket::new(
            packet_id,
            PubrelReasonCode::PacketIdentifierNotFound,
        ))
    }
}

pub(super) async fn handle_pubcomp<'a, S>(
    session: &mut Session,
    packet_id: u16,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a pubcomp packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage.pubcomp(session.client_id(), packet_id).await?;

    Ok(())
}

pub(super) async fn handle_will<'a, S>(
    session: &mut Session,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} handle last will:
client side disconnected : {}
server side disconnected : {}
               last will : {:?}"#,
        session.client_id(),
        session.client_disconnected(),
        session.server_disconnected(),
        session.last_will(),
    );

    if let Some(last_will) = session.take_last_will() {
        deliver_publish_message(session, last_will.into(), global, storage).await?;
        session.clear_last_will();
    }
    Ok(())
}

pub(crate) async fn retrieve_pending_messages<'a, S>(
    client_id: &str,
    storage: &'a Storage<S>,
) -> io::Result<Vec<VariablePacket>>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    let mut packets = Vec::new();
    let ret = storage.retrieve_pending_messages(client_id).await?;
    if let Some(messages) = ret {
        for msg in messages {
            match msg.pubrec_at() {
                Some(_) => {
                    packets.push(
                        PubcompPacket::new(msg.server_packet_id(), PubcompReasonCode::Success)
                            .into(),
                    );
                }
                None => {
                    let qos = match msg.final_qos() {
                        QualityOfService::Level1 => {
                            QoSWithPacketIdentifier::Level1(msg.server_packet_id())
                        }
                        QualityOfService::Level2 => {
                            QoSWithPacketIdentifier::Level2(msg.server_packet_id())
                        }
                        QualityOfService::Level0 => unreachable!(),
                    };
                    let topic_name = msg.message().topic_name().to_owned();
                    let mut packet = PublishPacket::new(topic_name, qos, msg.message().payload());
                    packet.set_dup(msg.message().dup());

                    packets.push(packet.into());
                }
            }
        }
    }

    Ok(packets)
}
