use std::io::{self, ErrorKind};

use kanal::{AsyncReceiver, AsyncSender};
use mqtt_codec_kit::v4::packet::{DisconnectPacket, PingrespPacket, VariablePacket};

use crate::{
    debug, error,
    protocols::v4::{connect::handle_disconnect, publish::handle_will},
    server::state::{DeliverMessage, GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

use super::{
    publish::{
        handle_deliver_publish, handle_puback, handle_pubcomp, handle_publish, handle_pubrec,
        handle_pubrel,
    },
    session::Session,
    subscribe::{handle_subscribe, handle_unsubscribe},
};

async fn remove_client<'a, S>(
    session: &Session,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    global.remove_client(session.client_id());
    if session.clean_session() {
        storage
            .unsubscribe_topics(session.client_id(), session.subscriptions())
            .await?;
        storage.clear_all_messages(session.client_id()).await?;
    }

    Ok(())
}

pub(super) async fn handle_read_packet<'a, S>(
    write_tx: &AsyncSender<VariablePacket>,
    session: &mut Session,
    packet: &VariablePacket,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<bool>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} receive mqtt client incoming message: {:?}"#,
        session.client_id(),
        packet,
    );

    session.renew_last_packet_at();
    let mut should_stop = false;
    match packet {
        VariablePacket::PingreqPacket(_packet) => {
            write_tx
                .send(PingrespPacket::new().into())
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
                write_tx.send(pkt).await.map_err(|err| {
                    error!("send publish response: {err}");
                    ErrorKind::InvalidData
                })?;
            }
            should_stop = stop;
        }
        VariablePacket::PubrelPacket(packet) => {
            let pkt = handle_pubrel(session, packet.packet_identifier(), global, storage).await?;
            debug!("write pubcomp packet: {:?}", pkt);
            write_tx.send(pkt.into()).await.map_err(|err| {
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
            write_tx.send(pkt.into()).await.map_err(|err| {
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
            write_tx.send(pkt.into()).await.map_err(|err| {
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

pub(super) async fn receive_deliver_message<'a, S>(
    session: &mut Session,
    packet: &DeliverMessage,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<(bool, Option<VariablePacket>)>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    let mut should_stop = false;
    let resp = match packet {
        DeliverMessage::Publish(subscribe_qos, packet) => {
            let resp = handle_deliver_publish(session, subscribe_qos, packet, storage).await?;
            if session.disconnected() {
                None
            } else {
                Some(resp.into())
            }
        }
        DeliverMessage::Online(s) => {
            debug!(
                "handle deliver client#{} receive new client online",
                session.client_id(),
            );

            if let Err(err) = s.send(session.server_packet_id()).await {
                error!(
                    "handle deliver client#{} send session state: {err}",
                    session.client_id(),
                );
            }

            remove_client(session, global, storage).await?;

            should_stop = true;

            if session.disconnected() {
                None
            } else {
                Some(DisconnectPacket::new().into())
            }
        }
        DeliverMessage::Kick(reason) => {
            debug!(
                "handle deliver client#{} receive kick message: {}",
                session.client_id(),
                reason,
            );

            if session.disconnected() && !session.clean_session() {
                None
            } else {
                remove_client(session, global, storage).await?;

                should_stop = true;

                Some(DisconnectPacket::new().into())
            }
        }
    };
    Ok((should_stop, resp))
}

pub(super) async fn handle_deliver_packet<'a, S>(
    sender: &AsyncSender<VariablePacket>,
    session: &mut Session,
    packet: &DeliverMessage,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<bool>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    let (should_stop, resp) = receive_deliver_message(session, packet, global, storage).await?;
    if let Some(packet) = resp {
        debug!("write packet: {:?}", packet);
        if let Err(err) = sender.send(packet).await {
            error!("write packet failed: {err}");
            return Ok(true);
        }
    }

    Ok(should_stop)
}

pub(super) async fn handle_clean_session<'a, S>(
    session: &mut Session,
    deliver_rx: &mut AsyncReceiver<DeliverMessage>,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} handle offline:
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

    while let Ok(p) = deliver_rx.recv().await {
        let (stop, _) = receive_deliver_message(session, &p, global, storage).await?;
        if stop {
            break;
        }
    }
    Ok(())
}
