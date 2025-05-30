use std::{collections::VecDeque, io};

use mqtt_codec_kit::{
    common::QualityOfService,
    v5::{
        control::DisconnectReasonCode,
        packet::{
            DisconnectPacket, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket,
            VariablePacket, suback::SubscribeReasonCode, subscribe::RetainHandling,
        },
    },
};

use crate::{
    debug,
    protocols::v5::common::build_error_disconnect,
    store::{Storage, message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
};

use super::{publish::handle_deliver_publish, session::Session};

pub(super) enum SubscribeAck {
    Success(Vec<VariablePacket>),
    Disconnect(DisconnectPacket),
}

pub(super) async fn handle_subscribe<'a, S>(
    session: &mut Session,
    packet: SubscribePacket,
    storage: &'a Storage<S>,
) -> io::Result<SubscribeAck>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
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
        return Ok(SubscribeAck::Disconnect(disconnect_packet));
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
        storage
            .subscribe(session.client_id(), filter, granted_qos)
            .await?;
        let exist = session.subscribe(filter.clone(), *subscribe_opts);

        // TODO: config: retain available?
        let send_retain = !filter.is_shared()
            && match subscribe_opts.retain_handling() {
                RetainHandling::SendAtSubscribe => true,
                RetainHandling::SendAtSubscribeIfNotExist => exist,
                RetainHandling::DoNotSend => false,
            };

        if send_retain {
            let retain_messages = RetainMessageStore::search(storage.as_ref(), filter).await?;
            for msg in retain_messages {
                if subscribe_opts.no_local() && msg.client_id().eq(session.client_id()) {
                    continue;
                }

                let mut packet =
                    handle_deliver_publish(session, granted_qos, &msg.into(), storage).await?;
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
    Ok(SubscribeAck::Success(queue.into()))
}

pub(super) async fn handle_unsubscribe<'a, S>(
    session: &mut Session,
    storage: &'a Storage<S>,
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
        packet.subscribes(),
    );

    let reason_codes = Vec::new();
    for filter in packet.subscribes() {
        session.unsubscribe(filter);
        storage.unsubscribe(session.client_id(), filter).await?;
    }

    Ok(UnsubackPacket::new(
        packet.packet_identifier(),
        reason_codes,
    ))
}
