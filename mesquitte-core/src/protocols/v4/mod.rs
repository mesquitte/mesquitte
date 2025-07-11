use futures::{SinkExt as _, StreamExt as _};
use kanal::bounded_async;
use mqtt_codec_kit::{
    common::{MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX},
    v4::{
        control::ConnectReturnCode,
        packet::{ConnackPacket, MqttDecoder, MqttEncoder, PublishPacket, VariablePacket},
    },
};
use nanoid::nanoid;
use read_loop::ReadLoop;
use session::Session;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{FramedRead, FramedWrite};
use write_loop::WriteLoop;

use crate::{
    debug, error,
    protocols::ProtocolSessionState,
    server::state::{AddClientReceipt, GlobalState},
    store::{
        message::{MessageStore, PendingPublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
    },
};

mod read_loop;
mod write_loop;

pub mod session;

pub(crate) struct EventLoop<R, W, S: 'static> {
    reader: R,
    writer: W,
    global: &'static GlobalState<S>,
}

impl<R, W, S> EventLoop<R, W, S>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(reader: R, writer: W, global: &'static GlobalState<S>) -> Self {
        Self {
            reader,
            writer,
            global,
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

        if packet.client_identifier().is_empty() && !packet.clean_session() {
            let _ = frame_writer
                .send(ConnackPacket::new(
                    false,
                    ConnectReturnCode::IdentifierRejected,
                ))
                .await;
            return;
        }

        // TODO: handle auth
        let client_id = if packet.client_identifier().is_empty() {
            nanoid!()
        } else {
            packet.client_identifier().to_string()
        };

        let mut session = Session::new(&client_id);
        session.set_clean_session(packet.clean_session());
        session.set_username(packet.username().map(|name| name.to_owned()));
        session.set_keep_alive(packet.keep_alive());

        if let Some(last_will) = packet.will() {
            let topic_name = last_will.topic();
            if topic_name.is_empty()
                || topic_name.contains(MATCH_ALL_STR)
                || topic_name.contains(MATCH_ONE_STR)
                || topic_name.starts_with(SHARED_PREFIX)
                || topic_name.starts_with(SYS_PREFIX)
            {
                debug!("handle connect last will topic is empty");
                let _ = frame_writer
                    .send(ConnackPacket::new(
                        false,
                        ConnectReturnCode::IdentifierRejected,
                    ))
                    .await;
                return;
            }

            // TODO: config: retain available
            // TODO: config: max qos
            session.set_last_will(last_will)
        }

        // FIXME: too many clients cause memory leak
        // TODO: config forward channel size
        let (forward_tx, forward_rx) = bounded_async(8);
        let receipt = self
            .global
            .add_client(session.client_id(), forward_tx)
            .await;
        let session_present = match receipt {
            AddClientReceipt::Present(state) => {
                let client_id = session.client_id();

                match state {
                    ProtocolSessionState::V4(session_state) => {
                        if !session.clean_session() {
                            session.copy_state(session_state);
                            true
                        } else {
                            debug!(
                                "packet id#{} session removed due to reconnect with clean session",
                                packet.client_identifier(),
                            );

                            for topic in session_state.subscriptions() {
                                if let Err(err) =
                                    self.global.storage.unsubscribe(client_id, topic).await
                                {
                                    debug!("handle connect unsubscribe old topic failed: {err}");
                                }
                            }

                            false
                        }
                    }

                    #[cfg(feature = "v5")]
                    ProtocolSessionState::V5(session_state) => {
                        for topic in session_state.subscriptions().keys() {
                            if let Err(err) =
                                self.global.storage.unsubscribe(client_id, topic).await
                            {
                                debug!("handle connect unsubscribe old topic failed: {err}");
                            }
                        }

                        false
                    }
                }
            }

            AddClientReceipt::New => false,
        };

        if let Err(err) = frame_writer
            .send(ConnackPacket::new(
                session_present,
                ConnectReturnCode::ConnectionAccepted,
            ))
            .await
        {
            error!("write connect ack error: {err}");
            return;
        }

        debug!("{session}");
        // TODO: config read write loop size
        let (read_tx, read_rx) = bounded_async(8);
        let mut read_task = tokio::spawn(ReadLoop::new(frame_reader, read_tx).read_from_client());
        let mut write_task = tokio::spawn(async {
            WriteLoop::new(frame_writer, read_rx, forward_rx, session, self.global)
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
