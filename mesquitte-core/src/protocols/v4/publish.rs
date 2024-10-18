use std::{cmp, io, sync::Arc};

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
    server::state::{DispatchMessage, GlobalState},
    store::{
        message::{IncomingPublishMessage, MessageStore, OutgoingPublishMessage},
        retain::RetainMessageStore,
        topic::{RouteOption, TopicStore},
        Storage,
    },
};

use super::session::Session;

pub(super) async fn handle_publish<MS, RS, TS>(
    session: &mut Session,
    packet: PublishPacket,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<(bool, Option<VariablePacket>)>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
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

    let topic_name = packet.topic_name();
    if topic_name.is_empty() {
        log::debug!("Publish topic name cannot be empty");
        return Ok((true, Some(DisconnectPacket::new().into())));
    }

    if topic_name.starts_with(SHARED_PREFIX)
        || topic_name.contains(MATCH_ALL_STR)
        || topic_name.contains(MATCH_ONE_STR)
    {
        log::debug!(
            "client#{} invalid topic name: {:?}",
            session.client_id(),
            topic_name
        );
        return Ok((true, Some(DisconnectPacket::new().into())));
    }
    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        log::debug!(
            "client#{} invalid duplicate flag in QoS 0 publish message",
            session.client_id()
        );
        return Ok((true, Some(DisconnectPacket::new().into())));
    }

    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {
            dispatch_publish(session, packet.into(), global, storage).await?;
            Ok((false, None))
        }
        QoSWithPacketIdentifier::Level1(packet_id) => {
            if !packet.dup() {
                dispatch_publish(session, packet.into(), global, storage).await?;
            }
            Ok((false, Some(PubackPacket::new(packet_id).into())))
        }
        QoSWithPacketIdentifier::Level2(packet_id) => {
            if !packet.dup() {
                storage
                    .message_store()
                    .enqueue_incoming(session.client_id(), packet_id, packet.into())
                    .await?;
            }
            Ok((false, Some(PubrecPacket::new(packet_id).into())))
        }
    }
}

// Dispatch a publish message from client or will to matched clients
pub(super) async fn dispatch_publish<MS, RS, TS>(
    session: &mut Session,
    packet: IncomingPublishMessage,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        r#"client#{} dispatch publish message:
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

    if packet.retain() {
        if packet.payload().is_empty() {
            storage
                .retain_message_store()
                .remove(packet.topic_name())
                .await?;
        } else {
            storage
                .retain_message_store()
                .insert((session.client_id(), &packet).into())
                .await?;
        }
    }
    let matches = storage.topic_store().search(packet.topic_name()).await?;
    let mut senders = Vec::with_capacity(matches.normal_clients.len());
    for (client_id, opt) in matches.normal_clients {
        match opt {
            RouteOption::V4(qos) => {
                senders.push((client_id.to_owned(), qos));
            }
            RouteOption::V5(subscribe_options) => {
                senders.push((client_id.to_owned(), subscribe_options.qos));
            }
        }
    }

    for (receiver_client_id, qos) in senders {
        if let Some(sender) = global.get_outgoing_sender(&receiver_client_id) {
            if sender.is_closed() {
                log::warn!(
                    "client#{:?} outgoing sender channel is closed",
                    receiver_client_id,
                );
                continue;
            }
            if let Err(err) = sender
                .send(DispatchMessage::Publish(qos, Box::new(packet.clone())))
                .await
            {
                log::error!("{} send publish message: {}", receiver_client_id, err,)
            }
        }
    }

    Ok(())
}

pub(super) async fn handle_pubrel<MS, RS, TS>(
    session: &mut Session,
    packet_id: u16,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<PubcompPacket>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        "client#{} received a pubrel packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage
        .message_store()
        .pubrel(session.client_id(), packet_id)
        .await?;

    Ok(PubcompPacket::new(packet_id))
}

pub(super) async fn receive_outgoing_publish<MS, RS, TS>(
    session: &mut Session,
    subscribe_qos: QualityOfService,
    message: IncomingPublishMessage,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<PublishPacket>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        r#"client#{} receive outgoing publish message:
topic name : {:?}
   payload : {:?}
     flags : publish qos={:?}, subscribe_qos={:?}, retain={}, dup={}"#,
        session.client_id(),
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
        let m = OutgoingPublishMessage::new(packet_id, subscribe_qos, message);
        storage
            .message_store()
            .enqueue_outgoing(session.client_id(), m)
            .await?;
    }

    Ok(packet)
}

pub(super) async fn handle_puback<MS, RS, TS>(
    session: &mut Session,
    packet_id: u16,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        "client#{} received a puback packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage
        .message_store()
        .puback(session.client_id(), packet_id)
        .await?;

    Ok(())
}

pub(super) async fn handle_pubrec<MS, RS, TS>(
    session: &mut Session,
    packet_id: u16,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<PubrelPacket>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        "client#{} received a pubrec packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage
        .message_store()
        .pubrec(session.client_id(), packet_id)
        .await?;

    Ok(PubrelPacket::new(packet_id))
}

pub(super) async fn handle_pubcomp<MS, RS, TS>(
    session: &mut Session,
    packet_id: u16,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
        "client#{} received a pubcomp packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage
        .message_store()
        .pubcomp(session.client_id(), packet_id)
        .await?;

    Ok(())
}

pub(super) async fn handle_will<MS, RS, TS>(
    session: &mut Session,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    log::debug!(
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
        dispatch_publish(session, last_will.into(), global, storage).await?;
        session.clear_last_will();
    }
    Ok(())
}

pub(crate) async fn get_outgoing_packets<MS, RS, TS>(
    session: &mut Session,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<Vec<PublishPacket>>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
{
    let mut packets = Vec::new();
    let messages = storage
        .message_store()
        .fetch_pending_outgoing(session.client_id())
        .await?;
    for msg in messages {
        let qos = match msg.final_qos() {
            QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
            QualityOfService::Level1 => QoSWithPacketIdentifier::Level1(msg.server_packet_id()),
            QualityOfService::Level2 => QoSWithPacketIdentifier::Level2(msg.server_packet_id()),
        };
        let topic_name = msg.message().topic_name().to_owned();
        let mut packet = PublishPacket::new(topic_name, qos, msg.message().payload());
        packet.set_dup(msg.message().dup());

        packets.push(packet);
    }

    Ok(packets)
}
