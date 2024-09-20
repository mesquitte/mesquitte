use std::{io, sync::Arc};

use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v4::packet::{
    DisconnectPacket, MqttDecoder, MqttEncoder, PingrespPacket, VariablePacket, VariablePacketError,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    protocols::v4::publish::receive_publish,
    server::state::GlobalState,
    types::{error::Error, outgoing::Outgoing, session::Session},
};

use super::{
    connect::{handle_connect, handle_disconnect, handle_offline},
    publish::{
        get_unsent_publish_packet, handle_puback, handle_pubcomp, handle_publish, handle_pubrec,
        handle_pubrel,
    },
    subscribe::{handle_subscribe, handle_unsubscribe},
};

async fn read_from_client<T, D>(mut reader: FramedRead<T, D>, msg_tx: mpsc::Sender<VariablePacket>)
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
                log::debug!("read from client: {:?}", packet);
                if let Err(err) = msg_tx.send(packet).await {
                    log::error!("receiver closed: {}", err);
                    break;
                }
            }
        }
    }
}

async fn handle_incoming<T, E>(
    session: &mut Session,
    writer: &mut FramedWrite<T, E>,
    global: Arc<GlobalState>,
    packet: VariablePacket,
    take_over: &mut bool,
) -> Result<bool, Error>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    session.renew_last_packet_at();
    let resp = match packet {
        VariablePacket::PingreqPacket(_packet) => PingrespPacket::new().into(),
        VariablePacket::PublishPacket(packet) => {
            match handle_publish(session, &packet, global.clone()).await {
                Ok(Some(resp)) => resp,
                Ok(None) => return Ok(false),
                Err(err) => return Err(err),
            }
        }
        VariablePacket::PubrelPacket(packet) => {
            handle_pubrel(session, global.clone(), packet.packet_identifier())
                .await
                .into()
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier());
            return Ok(false);
        }
        VariablePacket::PubrecPacket(packet) => {
            handle_pubrec(session, packet.packet_identifier()).into()
        }
        VariablePacket::SubscribePacket(packet) => {
            let packets = handle_subscribe(session, &packet, global.clone());
            for packet in packets {
                writer.send(packet).await?;
            }
            return Ok(false);
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier());
            return Ok(false);
        }
        VariablePacket::UnsubscribePacket(packet) => {
            handle_unsubscribe(session, &packet, global.clone()).into()
        }
        VariablePacket::DisconnectPacket(_packet) => {
            handle_disconnect(session).await;
            return Ok(true);
        }
        _ => {
            log::debug!("unsupported packet: {:?}", packet);
            *take_over = false;
            return Err(Error::UnsupportedPacket);
        }
    };
    writer.send(resp).await?;
    Ok(false)
}

async fn handle_control<T, E>(
    session: &mut Session,
    writer: &mut FramedWrite<T, E>,
    global: Arc<GlobalState>,
    packet: Outgoing,
    take_over: &mut bool,
) -> Result<bool, Error>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    let mut exists = false;
    let resp = match packet {
        Outgoing::Publish(msg) => receive_publish(session, msg).into(),
        Outgoing::Online(sender) => {
            log::debug!("new client#{} online", session.client_identifier());
            global.remove_client(session.client_id(), session.subscribes().keys());
            *take_over = false;

            if let Err(err) = sender.send(session.into()).await {
                log::error!(
                    "client#{} send session state : {}",
                    session.client_identifier(),
                    err,
                );
            }
            exists = true;

            DisconnectPacket::new().into()
        }
        Outgoing::Kick(reason) => {
            log::debug!(
                "client#{} kicked out: {}",
                session.client_identifier(),
                reason
            );
            global.remove_client(session.client_id(), session.subscribes().keys());
            *take_over = false;
            exists = true;

            DisconnectPacket::new().into()
        }
    };

    writer.send(resp).await?;
    Ok(exists)
}

async fn write_to_client<T, E>(
    mut writer: FramedWrite<T, E>,
    mut msg_rx: mpsc::Receiver<VariablePacket>,
    global: Arc<GlobalState>,
) -> Result<(), Error>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    let packet = match msg_rx.recv().await {
        Some(VariablePacket::ConnectPacket(packet)) => packet,
        _ => {
            log::debug!("first packet is not CONNECT packet");
            return Err(Error::InvalidConnectPacket);
        }
    };

    let (mut session, mut outgoing_rx) =
        match handle_connect(&mut writer, &packet, global.clone()).await {
            Ok(r) => r,
            Err(err) => {
                log::error!(
                    "handle client#{} connect: {err}",
                    packet.client_identifier()
                );
                return Err(err);
            }
        };

    let packets = get_unsent_publish_packet(&mut session);
    for packet in packets {
        writer.send(packet.into()).await?;
    }

    let mut take_over = true;
    loop {
        tokio::select! {
            packet = msg_rx.recv() => match packet {
                Some(packet) => {
                    let ret = handle_incoming(
                        &mut session,
                        &mut writer,
                        global.clone(),
                        packet,
                        &mut take_over,
                    )
                    .await;
                    match ret {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            log::error!("handle mqtt incoming message failed: {err}");
                            break;
                        }
                    }
                }
                None => {
                    log::warn!("incoming receive channel closed");
                    break;
                }
            },
            packet = outgoing_rx.recv() => match packet {
                Some(packet) => {
                    let ret = handle_control(
                        &mut session,
                        &mut writer,
                        global.clone(),
                        packet,
                        &mut take_over,
                    )
                    .await;
                    match ret {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(err) => {
                            log::error!("handle mqtt outgoing message failed: {err}");
                            break;
                        }
                    }
                }
                None => {
                    log::warn!("outgoing receive channel closed");
                    break;
                }
            }
        }
    }

    tokio::spawn(handle_offline(
        session,
        outgoing_rx,
        global.clone(),
        take_over,
    ));
    Ok(())
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

    let mut write_task = tokio::spawn(async move {
        if let Err(err) = write_to_client(frame_writer, msg_rx, global.clone()).await {
            log::error!("write to client: {err}");
        }
    });

    if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
        log::warn!("read_task/write_task terminated");
        read_task.abort();
    };
}
