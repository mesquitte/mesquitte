use std::{collections::VecDeque, io};

use mqtt_codec_kit::v4::packet::{
    suback::SubscribeReturnCode, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket,
    VariablePacket,
};

use crate::store::{
    message::MessageStore,
    retain::RetainMessageStore,
    topic::{RouteOption, TopicStore},
    Storage,
};

use super::{publish::handle_deliver_publish, session::Session};

pub(super) async fn handle_subscribe<S>(
    session: &mut Session,
    packet: &SubscribePacket,
    storage: &'static Storage<S>,
) -> io::Result<Vec<VariablePacket>>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    log::debug!(
        r#"client#{} received a subscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    let mut return_codes = Vec::with_capacity(packet.subscribes().len());
    let mut retain_packets: Vec<VariablePacket> = Vec::new();
    for (filter, subscribe_qos) in packet.subscribes() {
        if filter.is_shared() {
            log::warn!("mqtt v3.x don't support shared subscription");
            return_codes.push(SubscribeReturnCode::Failure);
            continue;
        }

        // TODO: granted max qos from config
        let granted_qos = subscribe_qos.to_owned();
        storage
            .inner
            .subscribe(session.client_id(), filter, RouteOption::V4(granted_qos))
            .await?;
        session.subscribe(filter.clone());

        let retain_messages = RetainMessageStore::search(&storage.inner, filter).await?;
        for msg in retain_messages {
            let mut packet =
                handle_deliver_publish(session, &granted_qos, &msg.into(), storage).await?;
            packet.set_retain(true);
            retain_packets.push(packet.into());
        }

        return_codes.push(granted_qos.into());
    }

    let mut queue: VecDeque<VariablePacket> = VecDeque::from(retain_packets);
    queue.push_front(SubackPacket::new(packet.packet_identifier(), return_codes).into());
    Ok(queue.into())
}

pub(super) async fn handle_unsubscribe<S>(
    session: &mut Session,
    store: &'static Storage<S>,
    packet: &UnsubscribePacket,
) -> io::Result<UnsubackPacket>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    log::debug!(
        r#"client#{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.topic_filters(),
    );
    for filter in packet.topic_filters() {
        session.unsubscribe(filter);
        store.inner.unsubscribe(session.client_id(), filter).await?;
    }

    Ok(UnsubackPacket::new(packet.packet_identifier()))
}
