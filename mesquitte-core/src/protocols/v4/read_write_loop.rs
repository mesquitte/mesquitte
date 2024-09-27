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
    server::state::GlobalState,
    types::{outgoing::Outgoing, session::Session},
};

use super::{
    connect::{handle_connect, handle_disconnect},
    publish::{
        get_unsent_outgoing_packet, handle_puback, handle_pubcomp, handle_publish, handle_pubrec,
        handle_pubrel, receive_outgoing_publish,
    },
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

pub(super) async fn handle_incoming<T, E>(
    writer: &mut FramedWrite<T, E>,
    session: &mut Session,
    packet: VariablePacket,
    global: Arc<GlobalState>,
) -> io::Result<bool>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    log::debug!(
        r#"client#{} receive mqtt client incoming message: {:?}"#,
        session.client_identifier(),
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
            let (stop, ack) = handle_publish(session, packet, global.clone()).await;
            if let Some(pkt) = ack {
                log::debug!("write puback packet: {:?}", pkt);
                writer.send(pkt).await?;
            }
            should_stop = stop;
        }
        VariablePacket::PubrelPacket(packet) => {
            let pkt = handle_pubrel(session, global.clone(), packet.packet_identifier()).await;
            log::debug!("write pubcomp packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier());
        }
        VariablePacket::PubrecPacket(packet) => {
            let pkt = handle_pubrec(session, packet.packet_identifier());
            log::debug!("write pubrel packet: {:?}", pkt);
            writer.send(pkt.into()).await?;
        }
        VariablePacket::SubscribePacket(packet) => {
            let packets = handle_subscribe(session, packet, global.clone());
            log::debug!("write suback packets: {:?}", packets);
            for pkt in packets {
                writer.send(pkt).await?;
            }
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier());
        }
        VariablePacket::UnsubscribePacket(packet) => {
            let pkt = handle_unsubscribe(session, &packet, global.clone());
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

pub(super) async fn receive_outgoing(
    session: &mut Session,
    packet: Outgoing,
    global: Arc<GlobalState>,
) -> (bool, Option<VariablePacket>) {
    let mut should_stop = false;
    let resp = match packet {
        Outgoing::Publish(subscribe_qos, packet) => {
            let resp = receive_outgoing_publish(session, subscribe_qos, packet);
            if session.disconnected() {
                None
            } else {
                Some(resp.into())
            }
        }
        Outgoing::Online(sender) => {
            log::debug!(
                "handle outgoing client#{} receive new client online",
                session.client_identifier(),
            );

            if let Err(err) = sender.send(session.into()).await {
                log::error!(
                    "handle outgoing client#{} send session state: {err}",
                    session.client_identifier(),
                );
            }
            should_stop = true;
            global.remove_client(session.client_id(), session.subscribes().keys());
            if session.disconnected() {
                None
            } else {
                Some(DisconnectPacket::new().into())
            }
        }
        Outgoing::Kick(reason) => {
            log::debug!(
                "handle outgoing client#{} receive kick message: {}",
                session.client_identifier(),
                reason,
            );

            if session.disconnected() && !session.clean_session() {
                None
            } else {
                should_stop = true;
                global.remove_client(session.client_id(), session.subscribes().keys());
                Some(DisconnectPacket::new().into())
            }
        }
    };
    (should_stop, resp)
}

pub(super) async fn handle_outgoing<T, E>(
    writer: &mut FramedWrite<T, E>,
    session: &mut Session,
    packet: Outgoing,
    global: Arc<GlobalState>,
) -> bool
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    let (should_stop, resp) = receive_outgoing(session, packet, global).await;
    if let Some(packet) = resp {
        log::debug!("write packet: {:?}", packet);
        if let Err(err) = writer.send(packet).await {
            log::error!("write packet failed: {err}");
            return true;
        }
    }

    should_stop
}

pub(super) async fn handle_clean_session(
    mut session: Session,
    mut outgoing_rx: mpsc::Receiver<Outgoing>,
    global: Arc<GlobalState>,
) {
    log::debug!(
        r#"client#{} handle offline:
 clean session : {}
    keep alive : {}"#,
        session.client_identifier(),
        session.clean_session(),
        session.keep_alive(),
    );
    if !session.disconnected() {
        session.set_server_disconnected();
    }

    if !session.client_disconnected() {
        handle_will(&mut session, global.clone()).await;
    }

    if session.clean_session() {
        global.remove_client(session.client_id(), session.subscribes().keys());
        return;
    }

    while let Some(p) = outgoing_rx.recv().await {
        let (stop, _) = receive_outgoing(&mut session, p, global.clone()).await;
        if stop {
            break;
        }
    }
}

async fn write_to_client<T, E>(
    mut writer: FramedWrite<T, E>,
    mut msg_rx: mpsc::Receiver<VariablePacket>,
    global: Arc<GlobalState>,
) where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    let packet = match msg_rx.recv().await {
        Some(VariablePacket::ConnectPacket(packet)) => packet,
        _ => {
            log::warn!("first packet is not CONNECT packet");
            return;
        }
    };

    let (mut session, mut outgoing_rx) =
        match handle_connect(&mut writer, packet, global.clone()).await {
            Ok(r) => r,
            Err(err) => {
                log::warn!("handle connect: {err}");
                return;
            }
        };

    let packets = get_unsent_outgoing_packet(&mut session);
    for packet in packets {
        if let Err(err) = writer.send(packet.into()).await {
            log::error!("write pending packet failed: {err}");
            return;
        }
    }
    if session.keep_alive() > 0 {
        let half_interval = Duration::from_millis(session.keep_alive() as u64 * 500);
        let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
        let keep_alive_timeout = half_interval * 3;
        loop {
            tokio::select! {
                packet = msg_rx.recv() => match packet {
                    Some(p) => match handle_incoming(&mut writer, &mut session, p, global.clone()).await {
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
                    Some(p) => if handle_outgoing(&mut writer, &mut session, p, global.clone()).await {
                        break;
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
                packet = msg_rx.recv() => match packet {
                    Some(p) => match handle_incoming(&mut writer, &mut session, p, global.clone()).await {
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
                    Some(p) => if handle_outgoing(&mut writer, &mut session, p, global.clone()).await {
                        break;
                    }
                    None => {
                        log::warn!("outgoing receive channel closed");
                        break;
                    }
                },
            }
        }
    };

    tokio::spawn(handle_clean_session(session, outgoing_rx, global.clone()));
}

pub async fn read_write_loop<R, W>(reader: R, writer: W, global: Arc<GlobalState>)
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let frame_reader = FramedRead::new(reader, MqttDecoder::new());
    let frame_writer = FramedWrite::new(writer, MqttEncoder::new());
    let (msg_tx, msg_rx) = mpsc::channel(8);
    let mut read_task = tokio::spawn(async move {
        read_from_client(frame_reader, msg_tx).await;
    });

    let mut write_task =
        tokio::spawn(async move { write_to_client(frame_writer, msg_rx, global.clone()).await });

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        log::warn!("read_task/write_task terminated");
        read_task.abort();
    };
}
