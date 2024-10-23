use std::{io, time::Duration};

use connect::handle_connect;
use futures::{SinkExt as _, StreamExt as _};
use mqtt_codec_kit::v4::packet::{MqttDecoder, MqttEncoder, VariablePacket, VariablePacketError};
use publish::fetch_pending_outgoing_messages;
use read_write_loop::{handle_clean_session, handle_incoming, handle_outgoing};
use session::Session;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::{interval_at, Instant},
};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};

use crate::{
    server::state::{DispatchMessage, GlobalState},
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

        let (mut session, outgoing_rx) = match handle_connect(&packet, self.global).await {
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

        match fetch_pending_outgoing_messages(&mut session, self.storage).await {
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
            ReadLoop::new(frame_reader, msg_tx).read_from_client().await;
        });

        let mut write_task = tokio::spawn(async move {
            WriteLoop::new(
                frame_writer,
                session,
                msg_rx,
                outgoing_rx,
                self.global,
                self.storage,
            )
            .write_to_client()
            .await
        });

        if tokio::try_join!(&mut read_task, &mut write_task).is_err() {
            log::warn!("read_task/write_task terminated");
            read_task.abort();
            write_task.abort();
        };
    }
}

struct ReadLoop<T, D> {
    reader: FramedRead<T, D>,
    sender: mpsc::Sender<VariablePacket>,
}

impl<T, D> ReadLoop<T, D>
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
{
    pub fn new(reader: FramedRead<T, D>, sender: mpsc::Sender<VariablePacket>) -> Self {
        Self { reader, sender }
    }

    async fn read_from_client(&mut self) {
        loop {
            match self.reader.next().await {
                None => {
                    log::info!("client closed");
                    break;
                }
                Some(Err(e)) => {
                    log::warn!("read from client: {}", e);
                    break;
                }
                Some(Ok(packet)) => {
                    if let Err(err) = self.sender.send(packet).await {
                        log::warn!("receiver closed: {}", err);
                        break;
                    }
                }
            }
        }
    }
}

pub struct WriteLoop<T, E, S: 'static> {
    writer: FramedWrite<T, E>,
    session: Session,
    incoming_rx: mpsc::Receiver<VariablePacket>,
    outgoing_rx: mpsc::Receiver<DispatchMessage>,
    global: &'static GlobalState,
    storage: &'static Storage<S>,
}

impl<T, E, S: 'static> WriteLoop<T, E, S>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        session: Session,
        incoming_rx: mpsc::Receiver<VariablePacket>,
        outgoing_rx: mpsc::Receiver<DispatchMessage>,
        global: &'static GlobalState,
        storage: &'static Storage<S>,
    ) -> Self {
        Self {
            writer,
            session,
            incoming_rx,
            outgoing_rx,
            global,
            storage,
        }
    }

    async fn write_to_client(mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        if self.session.keep_alive() > 0 {
            let half_interval = Duration::from_millis(self.session.keep_alive() as u64 * 500);
            let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
            let keep_alive_timeout = half_interval * 3;
            loop {
                tokio::select! {
                    packet = self.incoming_rx.recv() => match packet {
                        Some(p) => match handle_incoming(&mut self.writer, &mut self.session, p, self.global, self.storage).await {
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
                    packet = self.outgoing_rx.recv() => match packet {
                        Some(p) => match handle_outgoing(&mut self.writer, &mut self.session, p, self.global, self.storage).await {
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
                        if self.session.last_packet_at().elapsed() > keep_alive_timeout {
                            break;
                        }
                    },
                }
            }
        } else {
            loop {
                tokio::select! {
                    packet = self.incoming_rx.recv() => match packet {
                        Some(p) => match handle_incoming(&mut self.writer, &mut self.session, p, self.global, self.storage).await {
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
                    packet = self.outgoing_rx.recv() => match packet {
                        Some(p) => match handle_outgoing(&mut self.writer, &mut self.session, p, self.global, self.storage).await {
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
                handle_clean_session(self.session, self.outgoing_rx, self.global, self.storage)
                    .await
            {
                log::error!("handle clean session: {}", err);
            }
        });
    }
}
