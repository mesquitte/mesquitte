use std::{io, time::Duration};

use connect::handle_connect;
use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v4::packet::{MqttDecoder, MqttEncoder, VariablePacket, VariablePacketError};
use publish::retrieve_pending_messages;
use read_write_loop::{handle_clean_session, handle_deliver_packet, handle_read_packet};
use session::Session;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::{interval_at, Instant},
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    server::state::{DeliverMessage, GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore, Storage},
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
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
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
                log::error!("first packet is not CONNECT packet");
                return;
            }
        };

        let (mut session, deliver_rx) = match handle_connect(&packet, self.global).await {
            Ok((pkt, session, deliver_rx)) => {
                if let Err(err) = frame_writer.send(pkt).await {
                    log::error!("handle connect write connect ack: {err}");
                    return;
                }
                (session, deliver_rx)
            }
            Err(pkt) => {
                let _ = frame_writer
                    .send(pkt)
                    .await
                    .map_err(|e| log::error!("handle connect write connect ack: {e}"));
                return;
            }
        };

        log::debug!(
            r#"client#{} session:
        connect at : {:?}
     clean session : {}
        keep alive : {}
assigned_client_id : {}"#,
            session.client_id(),
            session.connected_at(),
            session.clean_session(),
            session.keep_alive(),
            session.assigned_client_id(),
        );

        match retrieve_pending_messages(&mut session, self.storage).await {
            Ok(packets) => {
                for pkt in packets {
                    if let Err(err) = frame_writer.send(pkt).await {
                        log::error!("write pending packet failed: {err}");
                        return;
                    }
                }
            }
            Err(err) => {
                log::error!("retrieve pending messages: {err}");
                return;
            }
        }

        let (write_tx, write_rx) = mpsc::channel(8);
        let client_id = session.client_id().to_owned();
        let mut write_task = tokio::spawn(async move {
            WriteLoop::new(frame_writer, client_id, write_rx)
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
            log::warn!("read_task/write_task terminated");
            write_task.abort();
        };
    }
}

struct ReadLoop<T, D, S: 'static> {
    reader: FramedRead<T, D>,
    write_tx: mpsc::Sender<VariablePacket>,
    deliver_rx: mpsc::Receiver<DeliverMessage>,
    session: Session,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<T, D, S: 'static> ReadLoop<T, D, S>
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub fn new(
        reader: FramedRead<T, D>,
        session: Session,
        deliver_rx: mpsc::Receiver<DeliverMessage>,
        write_tx: mpsc::Sender<VariablePacket>,
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
                        Some(Ok(p)) => match handle_read_packet(&self.write_tx, &mut self.session, p, self.global, self.storage).await {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                log::warn!("read form client handle message failed: {}", err);
                                break;
                            },
                        }
                        Some(Err(err)) => {
                            log::warn!("read form client failed: {}", err);
                            break;
                        }
                        None => {
                            log::warn!("incoming receive channel closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Some(p) => match handle_deliver_packet(&self.write_tx, &mut self.session, p, self.global, self.storage).await {
                            Ok(should_stop) => if should_stop {
                                break;
                            },
                            Err(err) => {
                                log::error!("handle deliver failed: {}", err);
                                break;
                            },
                        }
                        None => {
                            log::warn!("deliver channel closed");
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
                        Some(Ok(p)) => match handle_read_packet(&self.write_tx, &mut self.session, p, self.global, self.storage).await {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(err) => {
                                log::warn!("read form client handle message failed: {}", err);
                                break;
                            },
                        }
                        Some(Err(err)) => {
                            log::warn!("read form client failed: {}", err);
                            break;
                        }
                        None => {
                            log::warn!("incoming receive channel closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Some(p) => match handle_deliver_packet(&self.write_tx, &mut self.session, p, self.global, self.storage).await {
                            Ok(should_stop) => if should_stop {
                                break;
                            },
                            Err(err) => {
                                log::error!("handle deliver failed: {}", err);
                                break;
                            },
                        }
                        None => {
                            log::warn!("deliver receive channel closed");
                            break;
                        }
                    },
                }
            }
        };

        tokio::spawn(async move {
            if let Err(err) =
                handle_clean_session(self.session, self.deliver_rx, self.global, self.storage).await
            {
                log::error!("handle clean session: {}", err);
            }
        });
    }
}

pub struct WriteLoop<T, E> {
    writer: FramedWrite<T, E>,
    client_id: String,
    write_rx: mpsc::Receiver<VariablePacket>,
}

impl<T, E> WriteLoop<T, E>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        client_id: String,
        write_rx: mpsc::Receiver<VariablePacket>,
    ) -> Self {
        Self {
            writer,
            write_rx,
            client_id,
        }
    }

    async fn write_to_client(&mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        loop {
            // TODO: ticker resend pending QoS1/2 message
            match self.write_rx.recv().await {
                Some(packet) => {
                    if let Err(err) = self.writer.send(packet).await {
                        log::warn!("client#{} write failed: {}", self.client_id, err);
                        break;
                    }
                }
                None => {
                    log::info!("client#{} write task closed", self.client_id);
                    break;
                }
            }
        }
    }
}
