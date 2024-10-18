use std::{io, sync::Arc, time::Duration};

use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v4::packet::{
    DisconnectPacket, MqttDecoder, MqttEncoder, PingrespPacket, VariablePacket, VariablePacketError,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::{interval_at, Instant},
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    protocols::v4::publish::handle_will,
    server::state::{DispatchMessage, GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
};

use super::{
    connect::{handle_connect, handle_disconnect},
    publish::{
        get_outgoing_packets, handle_puback, handle_pubcomp, handle_publish, handle_pubrec,
        handle_pubrel, receive_outgoing_publish,
    },
    session::Session,
    subscribe::{handle_subscribe, handle_unsubscribe},
};

async fn read_from_client<T, D>(mut reader: FramedRead<T, D>, sender: mpsc::Sender<VariablePacket>)
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
{
    loop {
        match reader.next().await {
            None => {
                log::info!("client closed");
                break;
            }
            Some(Err(e)) => {
                log::warn!("read from client: {}", e);
                break;
            }
            Some(Ok(packet)) => {
                if let Err(err) = sender.send(packet).await {
                    log::warn!("receiver closed: {}", err);
                    break;
                }
            }
        }
    }
}

async fn remove_client<MS, RS, TS>(
    session: &Session,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    global.remove_client(session.client_id());
    storage
        .topic_store()
        .unsubscribe_topics(session.client_id(), session.subscriptions())
        .await?;

    Ok(())
}

pub(super) async fn handle_incoming<W, E, MS, RS, TS>(
    writer: &mut FramedWrite<W, E>,
    session: &mut Session,
    packet: VariablePacket,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<bool>
where
    W: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    log::debug!(
        r#"client#{} receive mqtt client incoming message: {:?}"#,
        session.client_id(),
        packet,
    );
    session.renew_last_packet_at();
    let mut should_stop = false;
    match packet {
        VariablePacket::PingreqPacket(_packet) => {
            let pkt = PingrespPacket::new();
            log::debug!("write pingresp packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::PublishPacket(packet) => {
            let (stop, ack) =
                handle_publish(session, packet, global.clone(), storage.clone()).await?;
            if let Some(pkt) = ack {
                log::debug!("write puback packet: {:?}", pkt);
                writer.send(pkt).await?;
            }
            should_stop = stop;
        }
        VariablePacket::PubrelPacket(packet) => {
            let pkt = handle_pubrel(session, packet.packet_identifier(), storage.clone()).await?;
            log::debug!("write pubcomp packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier(), storage.clone()).await?;
        }
        VariablePacket::PubrecPacket(packet) => {
            let pkt = handle_pubrec(session, packet.packet_identifier(), storage.clone()).await?;
            log::debug!("write pubrel packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            let packets = handle_subscribe(session, packet, storage.clone()).await?;
            log::debug!("write suback packets: {:?}", packets);
            for pkt in packets {
                writer.send(pkt).await?;
            }
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier(), storage.clone()).await?;
        }
        VariablePacket::UnsubscribePacket(packet) => {
            let pkt = handle_unsubscribe(session, storage.topic_store(), &packet).await?;
            log::debug!("write unsuback packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::DisconnectPacket(_packet) => {
            handle_disconnect(session).await;
            should_stop = true;
        }
        _ => {
            log::debug!("unsupported packet: {:?}", packet);
            should_stop = true;
        }
    };

    Ok(should_stop)
}

pub(super) async fn receive_outgoing<MS, RS, TS>(
    session: &mut Session,
    packet: DispatchMessage,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<(bool, Option<VariablePacket>)>
where
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    let mut should_stop = false;
    let resp = match packet {
        DispatchMessage::Publish(subscribe_qos, packet) => {
            let resp =
                receive_outgoing_publish(session, subscribe_qos, *packet, storage.clone()).await?;
            if session.disconnected() {
                None
            } else {
                Some(resp.into())
            }
        }
        DispatchMessage::Online(sender) => {
            log::debug!(
                "handle outgoing client#{} receive new client online",
                session.client_id(),
            );

            if let Err(err) = sender.send(session.server_packet_id()).await {
                log::error!(
                    "handle outgoing client#{} send session state: {err}",
                    session.client_id(),
                );
            }
            should_stop = true;

            remove_client(session, global.clone(), storage.clone()).await?;

            if session.disconnected() {
                None
            } else {
                Some(DisconnectPacket::new().into())
            }
        }
        DispatchMessage::Kick(reason) => {
            log::debug!(
                "handle outgoing client#{} receive kick message: {}",
                session.client_id(),
                reason,
            );

            if session.disconnected() && !session.clean_session() {
                None
            } else {
                should_stop = true;

                remove_client(session, global.clone(), storage.clone()).await?;

                Some(DisconnectPacket::new().into())
            }
        }
    };
    Ok((should_stop, resp))
}

pub(super) async fn handle_outgoing<T, E, MS, RS, TS>(
    writer: &mut FramedWrite<T, E>,
    session: &mut Session,
    packet: DispatchMessage,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<bool>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    let (should_stop, resp) = receive_outgoing(session, packet, global, storage).await?;
    if let Some(packet) = resp {
        log::debug!("write packet: {:?}", packet);
        if let Err(err) = writer.send(packet).await {
            log::error!("write packet failed: {err}");
            return Ok(true);
        }
    }

    Ok(should_stop)
}

pub(super) async fn handle_clean_session<MS, RS, TS>(
    mut session: Session,
    mut outgoing_rx: mpsc::Receiver<DispatchMessage>,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) -> io::Result<()>
where
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    log::debug!(
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
        handle_will(&mut session, global.clone(), storage.clone()).await?;
    }

    if session.clean_session() {
        remove_client(&session, global.clone(), storage.clone()).await?;
        return Ok(());
    }

    while let Some(p) = outgoing_rx.recv().await {
        let (stop, _) = receive_outgoing(&mut session, p, global.clone(), storage.clone()).await?;
        if stop {
            break;
        }
    }
    Ok(())
}

async fn write_to_client<T, E, MS, RS, TS>(
    mut session: Session,
    mut writer: FramedWrite<T, E>,
    mut incoming_rx: mpsc::Receiver<VariablePacket>,
    mut outgoing_rx: mpsc::Receiver<DispatchMessage>,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    MS: MessageStore + 'static,
    RS: RetainMessageStore + 'static,
    TS: TopicStore + 'static,
{
    session.set_clean_session(true);
    if session.keep_alive() > 0 {
        let half_interval = Duration::from_millis(session.keep_alive() as u64 * 500);
        let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
        let keep_alive_timeout = half_interval * 3;
        loop {
            tokio::select! {
                packet = incoming_rx.recv() => match packet {
                    Some(p) => match handle_incoming(&mut writer, &mut session, p, global.clone(), storage.clone()).await {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            log::error!("handle incoming failed: {err}");
                            break;
                        },
                    }
                    None => {
                        log::warn!("incoming receive channel closed");
                        break;
                    }
                },
                packet = outgoing_rx.recv() => match packet {
                    Some(p) => match handle_outgoing(&mut writer, &mut session, p, global.clone(), storage.clone()).await {
                        Ok(should_stop) => if should_stop {
                            break;
                        },
                        Err(err) => {
                            log::error!("handle outgoing failed: {}", err);
                            break;
                        },
                    }
                    None => {
                        log::warn!("outgoing receive channel closed");
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
                    Some(p) => match handle_incoming(&mut writer, &mut session, p, global.clone(), storage.clone()).await {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            log::error!("handle incoming failed: {err}");
                            break;
                        },
                    }
                    None => {
                        log::warn!("incoming receive channel closed");
                        break;
                    }
                },
                packet = outgoing_rx.recv() => match packet {
                    Some(p) => match handle_outgoing(&mut writer, &mut session, p, global.clone(), storage.clone()).await {
                        Ok(should_stop) => if should_stop {
                            break;
                        },
                        Err(err) => {
                            log::error!("handle outgoing failed: {}", err);
                            break;
                        },
                    }
                    None => {
                        log::warn!("outgoing receive channel closed");
                        break;
                    }
                },
            }
        }
    };

    tokio::spawn(async move {
        if let Err(err) =
            handle_clean_session(session, outgoing_rx, global.clone(), storage.clone()).await
        {
            log::error!("handle clean session: {}", err);
        }
    });
}

pub async fn read_write_loop<R, W, MS, RS, TS>(
    reader: R,
    writer: W,
    global: Arc<GlobalState>,
    storage: Arc<Storage<MS, RS, TS>>,
) where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    MS: MessageStore + 'static,
    RS: RetainMessageStore + 'static,
    TS: TopicStore + 'static,
{
    let mut frame_reader = FramedRead::new(reader, MqttDecoder::new());
    let mut frame_writer = FramedWrite::new(writer, MqttEncoder::new());

    let packet = match frame_reader.next().await {
        Some(Ok(VariablePacket::ConnectPacket(packet))) => packet,
        _ => {
            log::error!("first packet is not CONNECT packet");
            return;
        }
    };

    let (mut session, outgoing_rx) = match handle_connect(packet, global.clone()).await {
        Ok((pkt, session, outgoing_rx)) => {
            if let Err(err) = frame_writer.send(pkt).await {
                log::error!("handle connect write connect ack: {err}");
                return;
            }
            (session, outgoing_rx)
        }
        Err(pkt) => {
            let _ = frame_writer
                .send(pkt)
                .await
                .map_err(|e| log::error!("handle connect write connect ack: {e}"));
            return;
        }
    };

    match get_outgoing_packets(&mut session, storage.clone()).await {
        Ok(packets) => {
            for pkt in packets {
                if let Err(err) = frame_writer.send(pkt).await {
                    log::error!("write pending packet failed: {err}");
                    return;
                }
            }
        }
        Err(err) => {
            log::error!("get outgoing packets: {err}");
            return;
        }
    }

    let (msg_tx, msg_rx) = mpsc::channel(8);
    let mut read_task = tokio::spawn(async move {
        read_from_client(frame_reader, msg_tx).await;
    });

    let mut write_task = tokio::spawn(async move {
        write_to_client(
            session,
            frame_writer,
            msg_rx,
            outgoing_rx,
            global.clone(),
            storage.clone(),
        )
        .await;
    });

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        log::warn!("read_task/write_task terminated");
        read_task.abort();
    };
}
