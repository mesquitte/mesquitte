use std::{io, time::Duration};

use futures::{SinkExt as _, StreamExt as _};
use kanal::{bounded_async, AsyncReceiver, AsyncSender};
use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, QualityOfService},
    v4::packet::{
        MqttDecoder, MqttEncoder, PubcompPacket, PublishPacket, VariablePacket, VariablePacketError,
    },
};
use read_write_loop::{handle_clean_session, handle_deliver_packet, handle_read_packet};
use session::Session;
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

mod connect;
mod publish;
mod session;
mod subscribe;

pub mod read_write_loop;

pub struct EventLoop<R, W, S: 'static> {
    reader: R,
    writer: W,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<R, W, S> EventLoop<R, W, S>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        reader: R,
        writer: W,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Self {
        Self {
            reader,
            writer,
            global,
            storage,
        }
    }

    pub async fn run(self) {
        let mut frame_reader = FramedRead::new(self.reader, MqttDecoder::new());
        let mut frame_writer = FramedWrite::new(self.writer, MqttEncoder::new());

        let packet = match frame_reader.next().await {
            Some(Ok(VariablePacket::ConnectPacket(packet))) => packet,
            _ => {
                error!("first packet is not CONNECT packet");
                return;
            }
        };

        let (session, deliver_rx) = match Self::handle_connect(&packet, self.global).await {
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

        debug!("{session}");

        let (write_tx, write_rx) = bounded_async(8);
        let client_id = session.client_id().to_owned();
        let mut write_task = tokio::spawn(async move {
            WriteLoop::new(frame_writer, client_id, write_rx, self.storage)
                .write_to_client()
                .await
        });

        let mut read_task = tokio::spawn(
            ReadLoop::new(
                frame_reader,
                session,
                deliver_rx,
                write_tx,
                self.global,
                self.storage,
            )
            .read_from_client(),
        );

        if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
            error!("read_task/write_task terminated");
            read_task.abort();
            write_task.abort();
        };
    }
}

struct ReadLoop<T, D, S: 'static> {
    reader: FramedRead<T, D>,
    write_tx: AsyncSender<VariablePacket>,
    deliver_rx: AsyncReceiver<DeliverMessage>,
    session: Session,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<T, D, S> ReadLoop<T, D, S>
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        reader: FramedRead<T, D>,
        session: Session,
        deliver_rx: AsyncReceiver<DeliverMessage>,
        write_tx: AsyncSender<VariablePacket>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Self {
        Self {
            reader,
            session,
            deliver_rx,
            write_tx,
            global,
            storage,
        }
    }

    async fn read_from_client(mut self) {
        if self.session.keep_alive() > 0 {
            let half_interval = Duration::from_millis(self.session.keep_alive() as u64 * 500);
            let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
            let keep_alive_timeout = half_interval * 3;
            loop {
                tokio::select! {
                    packet = self.reader.next() => match packet {
                        Some(Ok(p)) => match handle_read_packet(&self.write_tx, &mut self.session, &p, self.global, self.storage).await {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                warn!("read form client handle message failed: {err}");
                                break;
                            },
                        }
                        Some(Err(err)) => {
                            warn!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            warn!("incoming receive channel closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(p) => match handle_deliver_packet(&self.write_tx, &mut self.session, &p, self.global, self.storage).await {
                            Ok(should_stop) => if should_stop {
                                break;
                            },
                            Err(err) => {
                                error!("handle deliver failed: {err}");
                                break;
                            },
                        }
                        Err(err) => {
                            warn!("deliver receive channel: {err}");
                            break;
                        }
                    },
                    _ = keep_alive_tick.tick() => {
                        if self.session.last_packet_at().elapsed() > keep_alive_timeout {
                            break;
                        }
                    },
                }
            }
        } else {
            loop {
                tokio::select! {
                    packet = self.reader.next() => match packet {
                        Some(Ok(p)) => match handle_read_packet(&self.write_tx, &mut self.session, &p, self.global, self.storage).await {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                warn!("read form client handle message failed: {err}");
                                break;
                            },
                        }
                        Some(Err(err)) => {
                            warn!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            warn!("incoming receive channel closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(p) => match handle_deliver_packet(&self.write_tx, &mut self.session, &p, self.global, self.storage).await {
                            Ok(should_stop) => if should_stop {
                                break;
                            },
                            Err(err) => {
                                error!("handle deliver failed: {err}");
                                break;
                            },
                        }
                        Err(err) => {
                            warn!("deliver receive channel: {err}");
                            break;
                        }
                    },
                }
            }
        };

        tokio::spawn(async move {
            if let Err(err) = handle_clean_session(
                &mut self.session,
                &mut self.deliver_rx,
                self.global,
                self.storage,
            )
            .await
            {
                error!("handle clean session: {err}");
            }
        });
    }
}

pub struct WriteLoop<'a, T, E, S> {
    writer: FramedWrite<T, E>,
    client_id: String,
    write_rx: AsyncReceiver<VariablePacket>,
    storage: &'a Storage<S>,
}

impl<'a, T, E, S> WriteLoop<'a, T, E, S>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        client_id: String,
        write_rx: AsyncReceiver<VariablePacket>,
        storage: &'a Storage<S>,
    ) -> Self {
        Self {
            writer,
            write_rx,
            client_id,
            storage,
        }
    }

    async fn write_to_client(&mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        // TODO: config: resend interval
        let interval = Duration::from_millis(500);
        let mut tick = interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                ret = self.write_rx.recv() => match ret {
                    Ok(packet) => {
                        if let Err(err) = self.writer.send(packet).await {
                            warn!("client#{} write failed: {}", self.client_id, err);
                            break;
                        }
                    }
                    Err(err) => {
                        info!("client#{} write channel: {err}", self.client_id);
                        break;
                    }
                },
                _ = tick.tick() => {
                    match self.storage.retrieve_pending_messages(&self.client_id).await {
                        Ok(None) => continue,
                        Ok(Some(messages)) => {
                            for msg in messages {
                                match msg.pubrec_at() {
                                    Some(_) => {
                                        if let Err(err) = self.writer.send(PubcompPacket::new(msg.server_packet_id()).into()).await {
                                            warn!("client#{} write pubcomp packet failed: {}", self.client_id, err);
                                            break;
                                        }
                                    }
                                    None => {
                                        let qos = match msg.final_qos() {
                                            QualityOfService::Level1 => QoSWithPacketIdentifier::Level1(msg.server_packet_id()),
                                            QualityOfService::Level2 => QoSWithPacketIdentifier::Level2(msg.server_packet_id()),
                                            QualityOfService::Level0 => unreachable!(),
                                        };
                                        let topic_name = msg.message().topic_name().to_owned();
                                        let mut packet = PublishPacket::new(topic_name, qos, msg.message().payload());
                                        // TODO: duplicate message？
                                        packet.set_dup(msg.dup());

                                        if let Err(err) = self.writer.send(packet.into()).await {
                                            warn!("client#{} write publish packet failed: {}", self.client_id, err);
                                            break;
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            error!("retrieve pending messages: {err}");
                            break;
                        },
                    }
                },
            }
        }
    }
}
