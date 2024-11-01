use std::{
    cmp,
    io::{self, ErrorKind},
};

use kanal::{AsyncReceiver, AsyncSender};
use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService},
    v4::packet::{PingrespPacket, VariablePacket},
};

use crate::{
    debug, error,
    protocols::{
        v4::{connect::handle_disconnect, publish::handle_will},
        ProtocolSessionState,
    },
    server::state::{DeliverMessage, GlobalState},
    store::{
        message::{MessageStore, PendingPublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
        Storage,
    },
};

use super::{
    publish::{handle_puback, handle_pubcomp, handle_publish, handle_pubrec, handle_pubrel},
    session::Session,
    subscribe::{handle_subscribe, handle_unsubscribe},
    WritePacket,
};

async fn remove_client<S>(
    session: &Session,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    if session.clean_session() {
        global.remove_client(session.client_id());
        for topic_filter in session.subscriptions() {
            storage
                .unsubscribe(session.client_id(), topic_filter)
                .await?;
        }
        storage.clear_all(session.client_id()).await?;
    }
    Ok(())
}

pub(super) async fn handle_read_packet<S>(
    write_tx: &AsyncSender<WritePacket>,
    session: &mut Session,
    packet: &VariablePacket,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<bool>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} read packet: {:?}"#,
        session.client_id(),
        packet,
    );

    session.renew_last_packet_at();
    let mut should_stop = false;
    match packet {
        VariablePacket::PingreqPacket(_packet) => {
            write_tx
                .send(WritePacket::VariablePacket(PingrespPacket::new().into()))
                .await
                .map_err(|err| {
                    error!("send ping response: {err}");
                    ErrorKind::InvalidData
                })?;
        }
        VariablePacket::PublishPacket(packet) => {
            let (stop, ack) = handle_publish(session, packet, global, storage).await?;
            if let Some(pkt) = ack {
                debug!("write puback packet: {:?}", pkt);
                write_tx
                    .send(WritePacket::VariablePacket(pkt))
                    .await
                    .map_err(|err| {
                        error!("send publish response: {err}");
                        ErrorKind::InvalidData
                    })?;
            }
            should_stop = stop;
        }
        VariablePacket::PubrelPacket(packet) => {
            let pkt = handle_pubrel(session, packet.packet_identifier(), global, storage).await?;
            debug!("write pubcomp packet: {:?}", pkt);
            write_tx
                .send(WritePacket::VariablePacket(pkt.into()))
                .await
                .map_err(|err| {
                    error!("send pubcomp response: {err}");
                    ErrorKind::InvalidData
                })?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier(), storage).await?;
        }
        VariablePacket::PubrecPacket(packet) => {
            let pkt = handle_pubrec(session, packet.packet_identifier(), storage).await?;
            debug!("write pubrel packet: {:?}", pkt);
            write_tx
                .send(WritePacket::VariablePacket(pkt.into()))
                .await
                .map_err(|err| {
                    error!("send pubrel response: {err}");
                    ErrorKind::InvalidData
                })?;
        }
        VariablePacket::SubscribePacket(packet) => {
            let packets = handle_subscribe(session, packet, storage).await?;
            debug!("write suback packets: {:?}", packets);
            for pkt in packets {
                write_tx.send(pkt).await.map_err(|err| {
                    error!("send subscribe response: {err}");
                    ErrorKind::InvalidData
                })?;
            }
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier(), storage).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            let pkt = handle_unsubscribe(session, storage, packet).await?;
            debug!("write unsuback packet: {:?}", pkt);
            write_tx
                .send(WritePacket::VariablePacket(pkt.into()))
                .await
                .map_err(|err| {
                    error!("send unsuback response: {err}");
                    ErrorKind::InvalidData
                })?;
        }
        VariablePacket::DisconnectPacket(_packet) => {
            handle_disconnect(session).await;
            should_stop = true;
        }
        _ => {
            debug!("unsupported packet: {:?}", packet);
            should_stop = true;
        }
    };

    Ok(should_stop)
}

pub(super) async fn handle_deliver_packet<S>(
    write_tx: &AsyncSender<WritePacket>,
    session: &mut Session,
    packet: DeliverMessage,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<bool>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    match packet {
        DeliverMessage::Publish(topic_filter, subscribe_qos, packet) => {
            debug!(
                r#"""client#{} receive deliver packet:
 topic filter : {:?},
subscribe qos : {:?},
       packet : {:?}"""#,
                session.client_id(),
                topic_filter,
                subscribe_qos,
                packet,
            );
            let final_qos = cmp::min(packet.qos(), subscribe_qos);
            let qos = match final_qos {
                QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                QualityOfService::Level1 => {
                    QoSWithPacketIdentifier::Level1(session.incr_server_packet_id())
                }
                QualityOfService::Level2 => {
                    QoSWithPacketIdentifier::Level2(session.incr_server_packet_id())
                }
            };
            let ret = write_tx
                .send(WritePacket::PendingMessage(PendingPublishMessage::new(
                    qos, *packet,
                )))
                .await;
            match ret {
                Ok(_) => Ok(false),
                Err(err) => {
                    error!("client#{} send session state: {err}", session.client_id());
                    Ok(true)
                }
            }
        }
        DeliverMessage::Online(sender) => {
            debug!("client#{} receive online message", session.client_id(),);
            if let Err(err) = sender
                .send(ProtocolSessionState::V4(session.build_state()))
                .await
            {
                error!("client#{} send session state: {err}", session.client_id());
            }

            remove_client(session, global, storage).await?;
            Ok(true)
        }
        DeliverMessage::Kick(reason) => {
            debug!(
                "client#{} receive kick message: {}",
                session.client_id(),
                reason,
            );
            remove_client(session, global, storage).await?;
            Ok(true)
        }
    }
}

pub(super) async fn handle_clean_session<S>(
    session: &mut Session,
    deliver_rx: &mut AsyncReceiver<DeliverMessage>,
    global: &GlobalState,
    storage: &Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} handle clean session:
 clean session : {}
    keep alive : {}"#,
        session.client_id(),
        session.clean_session(),
        session.keep_alive(),
    );
    if !session.disconnected() {
        session.set_server_disconnected();
    }

    if !session.client_disconnected() {
        handle_will(session, global, storage).await?;
    }

    if session.clean_session() {
        remove_client(session, global, storage).await?;
        return Ok(());
    }

    while let Ok(packet) = deliver_rx.recv().await {
        match packet {
            DeliverMessage::Publish(topic_filter, subscribe_qos, packet) => {
                debug!(
                    r#"""client#{} receive deliver packet:
     topic filter : {:?},
    subscribe qos : {:?},
           packet : {:?}"""#,
                    session.client_id(),
                    topic_filter,
                    subscribe_qos,
                    packet,
                );
                let final_qos = cmp::min(packet.qos(), subscribe_qos);
                let (packet_id, qos) = match final_qos {
                    QualityOfService::Level0 => continue,
                    QualityOfService::Level1 => {
                        let packet_id = session.incr_server_packet_id();
                        (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                    }
                    QualityOfService::Level2 => {
                        let packet_id = session.incr_server_packet_id();
                        (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                    }
                };

                let message = PendingPublishMessage::new(qos, *packet);
                storage
                    .save_pending_publish_message(session.client_id(), packet_id, message)
                    .await?;
            }
            DeliverMessage::Online(sender) => {
                debug!("client#{} receive online message", session.client_id(),);
                if let Err(err) = sender
                    .send(ProtocolSessionState::V4(session.build_state()))
                    .await
                {
                    error!("client#{} send session state: {err}", session.client_id(),);
                }

                remove_client(session, global, storage).await?;
                break;
            }
            DeliverMessage::Kick(reason) => {
                debug!(
                    "client#{} receive kick message: {}",
                    session.client_id(),
                    reason,
                );
                remove_client(session, global, storage).await?;
                break;
            }
        }
    }
    Ok(())
}
