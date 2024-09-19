use std::{io, sync::Arc};

use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v5::{
    control::DisconnectReasonCode,
    packet::{
        DisconnectPacket, MqttDecoder, MqttEncoder, PingrespPacket, VariablePacket,
        VariablePacketError,
    },
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    protocols::v5::publish::receive_publish,
    server::state::GlobalState,
    types::{error::Error, outgoing::Outgoing},
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
            packet = msg_rx.recv() => {
                match packet {
                    Some(packet) => {
                        session.renew_last_packet_at();
                        let resp = match packet {
                            VariablePacket::PingreqPacket(_packet) => PingrespPacket::new().into(),
                            VariablePacket::PublishPacket(packet) => {
                                match handle_publish(&mut session, &packet, global.clone()).await {
                                    Ok(Some(resp)) => resp,
                                    Ok(None) => continue,
                                    Err(err) => {
                                        log::error!("handle publish message failed: {}", err);
                                        break;
                                    }
                                }
                            }
                            VariablePacket::PubrelPacket(packet) => {
                                handle_pubrel(&mut session, global.clone(), packet.packet_identifier())
                                    .await
                                    .into()
                            }
                            VariablePacket::PubackPacket(packet) => {
                                handle_puback(&mut session, packet.packet_identifier());
                                continue;
                            }
                            VariablePacket::PubrecPacket(packet) => {
                                handle_pubrec(&mut session, packet.packet_identifier()).into()
                            }
                            VariablePacket::SubscribePacket(packet) => {
                                match handle_subscribe(&mut session, &packet, global.clone()) {
                                    Ok(packets) => {
                                        for packet in packets {
                                            if let Err(err) = writer.send(packet).await {
                                                log::error!(
                                                    "write subscribe ack to client#{} : {}",
                                                    &session.client_identifier(),
                                                    err
                                                );
                                                break;
                                            }
                                        }
                                        continue;
                                    },
                                    Err(packet) => {
                                        if let Err(err) = writer.send(packet.into()).await {
                                            log::error!(
                                                "write subscribe ack to client#{} : {}",
                                                &session.client_identifier(),
                                                err
                                            );
                                        }
                                        break;
                                    },
                                }

                            }
                            VariablePacket::PubcompPacket(packet) => {
                                handle_pubcomp(&mut session, packet.packet_identifier());
                                continue;
                            }
                            VariablePacket::UnsubscribePacket(packet) => {
                                handle_unsubscribe(&mut session, &packet, global.clone()).into()
                            }
                            VariablePacket::DisconnectPacket(_packet) => {
                                handle_disconnect(&mut session).await;
                                take_over = false;
                                break;
                            }
                            _ => {
                                log::debug!("unsupported packet: {:?}", packet);
                                take_over = false;
                                break;
                            }
                        };

                        if let Err(err) = writer.send(resp).await {
                            log::error!("write to client#{} : {}", &session.client_identifier(), err);
                            break;
                        }
                    }
                    None => {
                        log::warn!("incoming receive channel closed");
                        break;
                    }
                }
            }
            packet = outgoing_rx.recv() => {
                match packet {
                    Some(packet) => {
                        let resp = match packet {
                            Outgoing::Publish(msg) => receive_publish(&mut session, msg).into(),
                            Outgoing::Online(sender) => {
                                global.remove_client(session.client_id(), session.subscribes().keys());
                                if let Err(err) = sender.send((&mut session).into()).await {
                                    log::debug!(
                                        "client#{} send session state : {}",
                                        session.client_identifier(),
                                        err,
                                    );
                                }

                                if let Err(err) = writer
                                    .send(DisconnectPacket::new(DisconnectReasonCode::SessionTakenOver).into())
                                    .await
                                {
                                    log::debug!(
                                        "client#{} write disconnect packet : {}",
                                        session.client_identifier(),
                                        err,
                                    );
                                }
                                take_over = false;
                                break;
                            }
                            Outgoing::Kick(reason) => {
                                log::info!(
                                    "client#{} kicked out: {}",
                                    session.client_identifier(),
                                    reason
                                );
                                global.remove_client(session.client_id(), session.subscribes().keys());
                                if let Err(err) = writer
                                    .send(
                                        DisconnectPacket::new(DisconnectReasonCode::AdministrativeAction)
                                            .into(),
                                    )
                                    .await
                                {
                                    log::error!(
                                        "send client#{} disconnect: {}",
                                        session.client_identifier(),
                                        err
                                    );
                                }
                                take_over = false;
                                break;
                            }
                        };
                        if let Err(err) = writer.send(resp).await {
                            log::error!(
                                "write outgoing to client#{} : {}",
                                &session.client_identifier(),
                                err
                            );
                            break;
                        }
                    }
                    None => {
                        log::warn!("outgoing receive channel closed");
                        break;
                    }
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
        log::error!("read_task/write_task terminated");
        read_task.abort();
    };
}
