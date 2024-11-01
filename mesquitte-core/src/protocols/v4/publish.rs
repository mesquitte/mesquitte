use std::io;

use mqtt_codec_kit::{
    common::{
        qos::QoSWithPacketIdentifier, TopicFilter, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX,
    },
    v4::packet::{
        DisconnectPacket, PubackPacket, PubcompPacket, PublishPacket, PubrecPacket, PubrelPacket,
        VariablePacket,
    },
};

use crate::{
    debug, error,
    server::state::{DeliverMessage, GlobalState},
    store::{
        message::{MessageStore, PublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
        Storage,
    },
    warn,
};

use super::session::Session;

pub(super) async fn handle_publish<S>(
    session: &mut Session,
    packet: &PublishPacket,
    global: &GlobalState,
    storage: &Storage<S>,
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

    let topic_name = packet.topic_name();
    if topic_name.is_empty() {
        debug!("Publish topic name cannot be empty");
        return Ok((true, Some(DisconnectPacket::new().into())));
    }

    if topic_name.starts_with(SHARED_PREFIX)
        || topic_name.contains(MATCH_ALL_STR)
        || topic_name.contains(MATCH_ONE_STR)
    {
        debug!(
            "client#{} invalid topic name: {:?}",
            session.client_id(),
            topic_name
        );
        return Ok((true, Some(DisconnectPacket::new().into())));
    }
    if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
        debug!(
            "client#{} invalid duplicate flag in QoS 0 publish message",
            session.client_id()
        );
        return Ok((true, Some(DisconnectPacket::new().into())));
    }

    match packet.qos() {
        QoSWithPacketIdentifier::Level0 => {
            deliver_publish_message(session, &packet.into(), global, storage).await?;
            Ok((false, None))
        }
        QoSWithPacketIdentifier::Level1(packet_id) => {
            if !packet.dup() {
                deliver_publish_message(session, &packet.into(), global, storage).await?;
            }
            Ok((false, Some(PubackPacket::new(packet_id).into())))
        }
        QoSWithPacketIdentifier::Level2(packet_id) => {
            if !packet.dup() {
                storage
                    .save_publish_message(session.client_id(), packet_id, packet.into())
                    .await?;
            }
            Ok((false, Some(PubrecPacket::new(packet_id).into())))
        }
    }
}

pub(super) async fn deliver_publish_message<S>(
    session: &mut Session,
    packet: &PublishMessage,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} deliver publish message:
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
            storage.remove(packet.topic_name()).await?;
        } else {
            storage.insert((session.client_id(), packet).into()).await?;
        }
    }

    let subscribes = storage.match_topic(packet.topic_name()).await?;
    for topic_content in subscribes {
        let topic_filter = if let Some(topic_filter) = topic_content.topic_filter {
            match TopicFilter::new(topic_filter) {
                Ok(filter) => filter,
                Err(err) => {
                    error!("deliver publish message new topic filter: {err}");
                    continue;
                }
            }
        } else {
            continue;
        };
        for (client_id, subscribe_qos) in topic_content.clients {
            if let Some(sender) = global.get_deliver(&client_id) {
                if sender.is_closed() {
                    warn!("client#{:?} deliver channel is closed", client_id,);
                    continue;
                }
                if let Err(err) = sender
                    .send(DeliverMessage::Publish(
                        topic_filter.clone(),
                        subscribe_qos,
                        Box::new(packet.clone()),
                    ))
                    .await
                {
                    error!("{} send publish: {}", client_id, err,)
                }
            }
        }
    }

    Ok(())
}

pub(super) async fn handle_pubrel<S>(
    session: &mut Session,
    packet_id: u16,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<PubcompPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a pubrel packet, id : {}",
        session.client_id(),
        packet_id
    );

    if let Some(msg) = storage.pubrel(session.client_id(), packet_id).await? {
        deliver_publish_message(session, &msg, global, storage).await?;
    }

    Ok(PubcompPacket::new(packet_id))
}

pub(super) async fn handle_puback<S>(
    session: &mut Session,
    packet_id: u16,
    storage: &Storage<S>,
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

pub(super) async fn handle_pubrec<S>(
    session: &mut Session,
    packet_id: u16,
    storage: &Storage<S>,
) -> io::Result<PubrelPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        "client#{} received a pubrec packet, id : {}",
        session.client_id(),
        packet_id
    );

    storage.pubrec(session.client_id(), packet_id).await?;

    Ok(PubrelPacket::new(packet_id))
}

pub(super) async fn handle_pubcomp<S>(
    session: &mut Session,
    packet_id: u16,
    storage: &Storage<S>,
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

pub(super) async fn handle_will<S>(
    session: &mut Session,
    global: &GlobalState,
    storage: &Storage<S>,
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
        deliver_publish_message(session, &last_will.into(), global, storage).await?;
    }
    Ok(())
}
