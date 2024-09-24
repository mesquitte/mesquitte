use std::{io, sync::Arc};

use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v4::packet::{MqttDecoder, MqttEncoder, VariablePacket, VariablePacketError};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{server::state::GlobalState, types::error::Error};

use super::{
    common::WritePacket,
    connect::{handle_connect, handle_offline},
    message::{handle_incoming, handle_outgoing},
    publish::get_unsent_outgoing_packet,
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

    let packets = get_unsent_outgoing_packet(&mut session);
    for packet in packets {
        writer.send(packet.into()).await?;
    }

    let mut take_over = true;
    loop {
        tokio::select! {
            packet = msg_rx.recv() => match packet {
                Some(p) => {
                    session.renew_last_packet_at();
                    if let Some(write_packet) = handle_incoming(&mut session, global.clone(), p).await {
                        match write_packet {
                            WritePacket::Packet(write_packet) => writer.send(write_packet).await?,
                            WritePacket::Packets(write_packets) => {
                                for write_packet in write_packets {
                                    writer.send(write_packet).await?
                                }
                            },
                            WritePacket::Disconnect(write_packet) => {
                                writer.send(write_packet.into()).await?;
                                break;
                            },
                            WritePacket::Stop => break,
                        }
                    }
                }
                None => {
                    log::warn!("incoming receive channel closed");
                    break;
                }
            },
            packet = outgoing_rx.recv() => match packet {
                Some(p) => {
                    match handle_outgoing(&mut session, global.clone(), p).await {
                        WritePacket::Packet(write_packet) => writer.send(write_packet).await?,
                        WritePacket::Packets(write_packets) => {
                            for write_packet in write_packets {
                                writer.send(write_packet).await?
                            }
                        }
                        WritePacket::Stop => break,
                        WritePacket::Disconnect(write_packet) => {
                            writer.send(write_packet.into()).await?;
                            take_over = false;
                            break;
                        },
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
