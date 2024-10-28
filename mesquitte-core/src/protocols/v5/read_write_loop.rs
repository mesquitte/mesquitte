use std::{io, time::Duration};

use futures::{SinkExt as _, StreamExt as _};
use kanal::{bounded_async, AsyncReceiver, AsyncSender};
use mqtt_codec_kit::v5::{
    control::DisconnectReasonCode,
    packet::{
        DisconnectPacket, MqttDecoder, MqttEncoder, PingrespPacket, VariablePacket,
        VariablePacketError,
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::{interval_at, Instant},
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    debug, error, info,
    server::state::{DeliverMessage, GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
    warn,
};

use super::{
    connect::{handle_connect, handle_disconnect},
    publish::{
        handle_deliver_publish, handle_puback, handle_pubcomp, handle_publish, handle_pubrec,
        handle_pubrel, handle_will, retrieve_pending_messages,
    },
    session::Session,
    subscribe::{handle_subscribe, handle_unsubscribe, SubscribeAck},
};

async fn read_from_client<T, D>(mut reader: FramedRead<T, D>, sender: AsyncSender<VariablePacket>)
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
{
    loop {
        match reader.next().await {
            None => {
                info!("client closed");
                break;
            }
            Some(Err(e)) => {
                warn!("read from client: {}", e);
                break;
            }
            Some(Ok(packet)) => {
                if let Err(err) = sender.send(packet).await {
                    warn!("receiver closed: {err}");
                    break;
                }
            }
        }
    }
}

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

pub(super) async fn handle_read_packet<'a, W, E, S>(
    writer: &mut FramedWrite<W, E>,
    session: &mut Session,
    packet: VariablePacket,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
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
            let pkt = PingrespPacket::new();
            debug!("write pingresp packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::PublishPacket(packet) => {
            let (stop, ack) = handle_publish(session, &packet, global, storage).await?;
            if let Some(pkt) = ack {
                debug!("write puback packet: {:?}", pkt);
                writer.send(pkt).await?;
            }
            should_stop = stop;
        }
        VariablePacket::PubrelPacket(packet) => {
            let pkt = handle_pubrel(session, packet.packet_identifier(), storage).await?;
            debug!("write pubcomp packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier(), storage).await?;
        }
        VariablePacket::PubrecPacket(packet) => {
            let pkt = handle_pubrec(session, packet.packet_identifier(), storage).await?;
            debug!("write pubrel packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            let ret = handle_subscribe(session, packet, storage).await?;
            match ret {
                SubscribeAck::Success(packets) => {
                    debug!("write suback packets: {:?}", packets);
                    for pkt in packets {
                        writer.send(pkt).await?;
                    }
                }
                SubscribeAck::Disconnect(pkt) => {
                    debug!("write disconnect packet: {:?}", pkt);
                    writer.send(pkt.into()).await?;
                    should_stop = true;
                }
            }
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier(), storage).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            let pkt = handle_unsubscribe(session, storage, &packet).await?;
            debug!("write unsuback packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::DisconnectPacket(packet) => {
            if let Some(pkt) = handle_disconnect(session, packet).await {
                debug!("write disconnect packet: {:?}", pkt);
                writer.send(pkt.into()).await?;
            }
            should_stop = true;
        }
        VariablePacket::AuthPacket(_packet) => {
            unimplemented!()
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
    packet: DeliverMessage,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<(bool, Option<VariablePacket>)>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    let mut should_stop = false;
    let resp = match packet {
        DeliverMessage::Publish(subscribe_qos, packet) => {
            let resp = handle_deliver_publish(session, subscribe_qos, &packet, storage).await?;
            if session.disconnected() {
                None
            } else {
                Some(resp.into())
            }
        }
        DeliverMessage::Online(sender) => {
            debug!(
                "handle deliver client#{} receive new client online",
                session.client_id(),
            );

            if let Err(err) = sender.send(session.server_packet_id()).await {
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
                Some(DisconnectPacket::new(DisconnectReasonCode::SessionTakenOver).into())
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

                Some(DisconnectPacket::new(DisconnectReasonCode::AdministrativeAction).into())
            }
        }
    };
    Ok((should_stop, resp))
}

pub(super) async fn handle_deliver_packet<'a, T, E, S>(
    writer: &mut FramedWrite<T, E>,
    session: &mut Session,
    packet: DeliverMessage,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<bool>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    let (should_stop, resp) = receive_deliver_message(session, packet, global, storage).await?;
    if let Some(packet) = resp {
        debug!("write packet: {:?}", packet);
        if let Err(err) = writer.send(packet).await {
            error!("write packet failed: {err}");
            return Ok(true);
        }
    }

    Ok(should_stop)
}

pub(super) async fn handle_clean_session<'a, S>(
    mut session: Session,
    deliver_rx: AsyncReceiver<DeliverMessage>,
    global: &'a GlobalState,
    storage: &'a Storage<S>,
) -> io::Result<()>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    debug!(
        r#"client#{} handle offline:
 clean session : {}
    keep alive : {}
session expiry : {}"#,
        session.client_id(),
        session.clean_session(),
        session.keep_alive(),
        session.session_expiry_interval(),
    );
    if !session.disconnected() {
        session.set_server_disconnected();
    }

    if !session.client_disconnected() {
        handle_will(&mut session, global, storage).await?;
    }

    if session.session_expiry_interval() > 0 {
        let dur = Duration::from_secs(session.session_expiry_interval() as u64);
        let mut tick = interval_at(Instant::now() + dur, dur);

        loop {
            tokio::select! {
                out = deliver_rx.recv() => {
                    match out {
                        Ok(p) => {
                            let (stop, _) = receive_deliver_message(&mut session, p, global, storage).await?;
                            if stop {
                                break;
                            }
                        },
                        Err(err) => {
                            error!("handle deliver failed: {err}");
                            break;
                        },
                    }
                }
                _ = tick.tick() => {
                    debug!("handle clean session client#{} session expired", session.client_id());
                    break;
                }
            }
        }
    } else {
        if session.clean_session() {
            remove_client(&session, global, storage).await?;
            return Ok(());
        }

        while let Ok(p) = deliver_rx.recv().await {
            let (stop, _) = receive_deliver_message(&mut session, p, global, storage).await?;
            if stop {
                break;
            }
        }
    }
    Ok(())
}

async fn write_to_client<T, E, S>(
    mut session: Session,
    mut writer: FramedWrite<T, E>,
    incoming_rx: AsyncReceiver<VariablePacket>,
    deliver_rx: AsyncReceiver<DeliverMessage>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
) where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    if session.keep_alive() > 0 {
        let half_interval = Duration::from_millis(session.keep_alive() as u64 * 500);
        let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
        let keep_alive_timeout = half_interval * 3;
        loop {
            tokio::select! {
                packet = incoming_rx.recv() => match packet {
                    Ok(p) => match handle_read_packet(&mut writer, &mut session, p, global, storage).await {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            error!("handle incoming failed: {err}");
                            break;
                        },
                    }
                    Err(err) => {
                        info!("client#{} receive channel: {err}", session.client_id());
                        break;
                    }
                },
                packet = deliver_rx.recv() => match packet {
                    Ok(p) => match handle_deliver_packet(&mut writer, &mut session, p, global, storage).await {
                        Ok(should_stop) => if should_stop {
                            break;
                        },
                        Err(err) => {
                            error!("handle deliver failed: {err}");
                            break;
                        },
                    }
                    Err(err) => {
                        info!("client#{} deliver channel: {err}", session.client_id());
                        break;
                    }
                },
                _ = keep_alive_tick.tick() => {
                    if session.last_packet_at().elapsed() > keep_alive_timeout {
                        break;
                    }
                },
            }
        }
    } else {
        loop {
            tokio::select! {
                packet = incoming_rx.recv() => match packet {
                    Ok(p) => match handle_read_packet(&mut writer, &mut session, p, global, storage).await {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            error!("handle incoming failed: {err}");
                            break;
                        },
                    }
                    Err(err) => {
                        info!("client#{} receive channel: {err}", session.client_id());
                        break;
                    }
                },
                packet = deliver_rx.recv() => match packet {
                    Ok(p) => match handle_deliver_packet(&mut writer, &mut session, p, global, storage).await {
                        Ok(should_stop) => if should_stop {
                            break;
                        },
                        Err(err) => {
                            error!("handle deliver failed: {err}");
                            break;
                        },
                    }
                    Err(err) => {
                        info!("client#{} deliver channel: {err}", session.client_id());
                        break;
                    }
                },
            }
        }
    };

    tokio::spawn(async move {
        if let Err(err) = handle_clean_session(session, deliver_rx, global, storage).await {
            error!("handle clean session: {err}");
        }
    });
}

pub async fn read_write_loop<R, W, S>(
    reader: R,
    writer: W,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    let mut frame_reader = FramedRead::new(reader, MqttDecoder::new());
    let mut frame_writer = FramedWrite::new(writer, MqttEncoder::new());

    let packet = match frame_reader.next().await {
        Some(Ok(VariablePacket::ConnectPacket(packet))) => packet,
        _ => {
            error!("first packet is not CONNECT packet");
            return;
        }
    };

    let (session, deliver_rx) = match handle_connect(packet, global).await {
        Ok((pkt, session, deliver_rx)) => {
            if let Err(err) = frame_writer.send(pkt).await {
                error!("handle connect write connect ack: {err}");
                return;
            }
            (session, deliver_rx)
        }
        Err(pkt) => {
            if let Err(err) = frame_writer.send(pkt).await {
                error!("handle connect write connect ack: {err}");
            }
            return;
        }
    };

    match retrieve_pending_messages(session.client_id(), storage).await {
        Ok(packets) => {
            for pkt in packets {
                if let Err(err) = frame_writer.send(pkt).await {
                    error!("write pending packet failed: {err}");
                    return;
                }
            }
        }
        Err(err) => {
            error!("retrieve pending messages: {err}");
            return;
        }
    }

    let (msg_tx, msg_rx) = bounded_async(8);
    let mut read_task = tokio::spawn(async move {
        read_from_client(frame_reader, msg_tx).await;
    });

    let mut write_task = tokio::spawn(async move {
        write_to_client(session, frame_writer, msg_rx, deliver_rx, global, storage).await;
    });

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        warn!("read_task/write_task terminated");
        read_task.abort();
        write_task.abort();
    };
}
