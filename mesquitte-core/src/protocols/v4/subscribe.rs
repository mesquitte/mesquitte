use std::{collections::VecDeque, io, sync::Arc};

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

use super::{publish::receive_outgoing_publish, session::Session};

pub(super) async fn handle_subscribe<MS, RS, TS>(
    session: &mut Session,
    packet: SubscribePacket,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<Vec<VariablePacket>>
where
    MS: MessageStore + Sync + Send,
    RS: RetainMessageStore + Sync + Send,
    TS: TopicStore + Sync + Send,
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
            .topic_store()
            .subscribe(session.client_id(), filter, RouteOption::V4(granted_qos))
            .await?;
        session.subscribe(filter.clone());

        let retain_messages = storage.retain_message_store().search(filter).await?;
        for msg in retain_messages {
            let mut packet =
                receive_outgoing_publish(session, granted_qos, msg.into(), storage.clone()).await?;
            packet.set_retain(true);
            retain_packets.push(packet.into());
        }

        return_codes.push(granted_qos.into());
    }

    let mut queue: VecDeque<VariablePacket> = VecDeque::from(retain_packets);
    queue.push_front(SubackPacket::new(packet.packet_identifier(), return_codes).into());
    Ok(queue.into())
}

pub(super) async fn handle_unsubscribe<TS>(
    session: &mut Session,
    topic_store: &TS,
    packet: &UnsubscribePacket,
) -> io::Result<UnsubackPacket>
where
    TS: TopicStore,
{
    log::debug!(
        r#"client#{} received a unsubscribe packet:
packet id : {}
   topics : {:?}"#,
        session.client_id(),
        packet.packet_identifier(),
        packet.subscribes(),
    );
    for filter in packet.subscribes() {
        session.unsubscribe(filter);
        topic_store.unsubscribe(session.client_id(), filter).await?;
    }

    Ok(UnsubackPacket::new(packet.packet_identifier()))
}
