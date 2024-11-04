use std::{io, time::Duration};

use futures::{SinkExt as _, StreamExt as _};
use kanal::{bounded_async, AsyncReceiver, AsyncSender};
use mqtt_codec_kit::{
    common::qos::QoSWithPacketIdentifier,
    v4::packet::{
        MqttDecoder, MqttEncoder, PublishPacket, PubrelPacket, VariablePacket, VariablePacketError,
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
    store::{
        message::{MessageStore, PendingPublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
        Storage,
    },
    warn,
};

mod connect;
mod publish;
mod subscribe;

pub mod read_write_loop;
pub mod session;

#[derive(Debug)]
pub enum WritePacket {
    VariablePacket(VariablePacket),
    PendingMessage(PendingPublishMessage),
}

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

        let (session, deliver_rx) =
            match Self::handle_connect(&packet, &self.storage, self.global).await {
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

        let mut write_task = tokio::spawn(async move {
            WriteLoop::new(frame_writer, client_id, write_rx, self.storage)
                .write_to_client()
                .await
        });

        if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
            error!("read_task/write_task terminated");
            read_task.abort();
            write_task.abort();
        };
    }
}

struct ReadLoop<T, D, S: 'static> {
    reader: FramedRead<T, D>,
    write_tx: AsyncSender<WritePacket>,
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
        write_tx: AsyncSender<WritePacket>,
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
                        Some(Ok(p)) => match handle_read_packet(
                            &self.write_tx,
                            &mut self.session,
                            &p,
                            self.global,
                            self.storage,
                        )
                        .await
                        {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                warn!("read form client handle message failed: {err}");
                                break;
                            }
                        },
                        Some(Err(err)) => {
                            warn!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            warn!("reader closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(packet) => match handle_deliver_packet(
                            &self.write_tx,
                            &mut self.session,
                            packet,
                            self.global,
                            self.storage,
                        )
                        .await
                        {
                            Ok(should_stop) => {
                                if should_stop {
                                    break;
                                }
                            }
                            Err(err) => {
                                error!("handle deliver failed: {err}");
                                break;
                            }
                        },
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
                        Some(Ok(p)) => match handle_read_packet(
                            &self.write_tx,
                            &mut self.session,
                            &p,
                            self.global,
                            self.storage,
                        )
                        .await
                        {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                warn!("read form client handle message failed: {err}");
                                break;
                            }
                        },
                        Some(Err(err)) => {
                            warn!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            warn!("reader closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(packet) => match handle_deliver_packet(
                            &self.write_tx,
                            &mut self.session,
                            packet,
                            self.global,
                            self.storage,
                        )
                        .await
                        {
                            Ok(should_stop) => {
                                if should_stop {
                                    break;
                                }
                            }
                            Err(err) => {
                                error!("handle deliver failed: {err}");
                                break;
                            }
                        },
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

pub struct WriteLoop<T, E, S: 'static> {
    writer: FramedWrite<T, E>,
    client_id: String,
    write_rx: AsyncReceiver<WritePacket>,
    storage: &'static Storage<S>,
}

impl From<PendingPublishMessage> for PublishPacket {
    fn from(value: PendingPublishMessage) -> Self {
        let mut pkt = PublishPacket::new(
            value.message().topic_name().to_owned(),
            value.qos(),
            value.message().payload(),
        );
        pkt.set_dup(value.dup());
        pkt.set_retain(value.message().retain());

        pkt
    }
}

impl From<&PendingPublishMessage> for PublishPacket {
    fn from(value: &PendingPublishMessage) -> Self {
        let mut pkt = PublishPacket::new(
            value.message().topic_name().to_owned(),
            value.qos(),
            value.message().payload(),
        );
        pkt.set_dup(value.dup());
        pkt.set_retain(value.message().retain());

        pkt
    }
}

impl<T, E, S> WriteLoop<T, E, S>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        client_id: String,
        write_rx: AsyncReceiver<WritePacket>,
        storage: &'static Storage<S>,
    ) -> Self {
        Self {
            writer,
            write_rx,
            client_id,
            storage,
        }
    }

    async fn write_pending_messages(&mut self, all: bool) -> bool {
        let ret = if all {
            self.storage.get_all_pending_messages(&self.client_id).await
        } else {
            self.storage.try_get_pending_messages(&self.client_id).await
        };

        match ret {
            Ok(Some(messages)) => {
                for (packet_id, pending_message) in messages {
                    match pending_message.pubrec_at() {
                        Some(_) => {
                            if let Err(err) =
                                self.writer.send(PubrelPacket::new(packet_id).into()).await
                            {
                                warn!(
                                    "client#{} write pubcomp packet failed: {}",
                                    self.client_id, err
                                );
                                return true;
                            }
                        }
                        None => {
                            let pkt: PublishPacket = pending_message.into();
                            if let Err(err) = self.writer.send(pkt.into()).await {
                                warn!(
                                    "client#{} write publish packet failed: {}",
                                    self.client_id, err
                                );
                                return true;
                            }
                        }
                    }
                }
                false
            }
            Ok(None) => false,
            Err(err) => {
                warn!("get pending messages failed: {err}");
                true
            }
        }
    }

    async fn write_to_client(&mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        if self.write_pending_messages(true).await {
            return;
        }
        // TODO: config: resend interval
        let interval = Duration::from_millis(500);
        let mut tick = interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                ret = self.write_rx.recv() => match ret {
                    Ok(message) => {
                        match message {
                            WritePacket::VariablePacket(pkt) => {
                                if let Err(err) = self.writer.send(pkt).await {
                                    warn!("client#{} write failed: {}", self.client_id, err);
                                    break;
                                }
                            }
                            WritePacket::PendingMessage(pending_message) => {
                                let pkt: PublishPacket = (&pending_message).into();
                                if let Err(err) = self.writer.send(pkt.into()).await {
                                    warn!("client#{} write failed: {}", self.client_id, err);
                                    break;
                                }

                                let packet_id = match pending_message.qos() {
                                    QoSWithPacketIdentifier::Level1(packet_id) => packet_id,
                                    QoSWithPacketIdentifier::Level2(packet_id) => packet_id,
                                    _ => continue,
                                };
                                if let Err(err) = self
                                    .storage
                                    .save_pending_publish_message(&self.client_id, packet_id, pending_message)
                                    .await
                                {
                                    error!("save pending publish message: {err}");
                                    break;
                                }
                            }
                        }
                    }
                    Err(err) => {
                        info!("client#{} write channel: {err}", self.client_id);
                        break;
                    }
                },
                _ = tick.tick() => {
                    if self.write_pending_messages(false).await {
                        break;
                    }
                },
            }
        }
    }
}
