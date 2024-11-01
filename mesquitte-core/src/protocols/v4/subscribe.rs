use std::{collections::VecDeque, io};

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService},
    v4::packet::{
        suback::SubscribeReturnCode, SubackPacket, SubscribePacket, UnsubackPacket,
        UnsubscribePacket,
    },
};

use crate::{
    debug,
    store::{
        message::{MessageStore, PendingPublishMessage, PublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
        Storage,
    },
    warn,
};

use super::{session::Session, WritePacket};

pub(super) async fn handle_subscribe<S>(
    session: &mut Session,
    packet: &SubscribePacket,
    storage: &Storage<S>,
) -> io::Result<Vec<WritePacket>>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    let mut retain_packets = Vec::new();
    for (filter, subscribe_qos) in packet.subscribes() {
        if filter.is_shared() {
            warn!("mqtt v3.x don't support shared subscription");
            return_codes.push(SubscribeReturnCode::Failure);
            continue;
        }

        // TODO: granted max qos from config
        let granted_qos = subscribe_qos.to_owned();
        storage
            .subscribe(session.client_id(), filter, granted_qos)
            .await?;
        session.subscribe(filter.clone());
        let retain_messages = RetainMessageStore::search(storage.as_ref(), filter).await?;
        for msg in retain_messages {
            let qos = match granted_qos {
                QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                QualityOfService::Level1 => {
                    QoSWithPacketIdentifier::Level1(session.incr_server_packet_id())
                }
                QualityOfService::Level2 => {
                    QoSWithPacketIdentifier::Level2(session.incr_server_packet_id())
                }
            };
            let mut received_publish: PublishMessage = msg.into();
            received_publish.set_retain(true);

            let pending_message = PendingPublishMessage::new(qos, received_publish);
            retain_packets.push(WritePacket::PendingMessage(pending_message));
        }

        return_codes.push(granted_qos.into());
    }

    let mut queue: VecDeque<WritePacket> = VecDeque::from(retain_packets);
    queue.push_front(WritePacket::VariablePacket(
        SubackPacket::new(packet.packet_identifier(), return_codes).into(),
    ));
    Ok(queue.into())
}

pub(super) async fn handle_unsubscribe<S>(
    session: &mut Session,
    store: &Storage<S>,
    packet: &UnsubscribePacket,
) -> io::Result<UnsubackPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.topic_filters(),
    );
    for filter in packet.topic_filters() {
        session.unsubscribe(filter);
        store.unsubscribe(session.client_id(), filter).await?;
    }

    Ok(UnsubackPacket::new(packet.packet_identifier()))
}
