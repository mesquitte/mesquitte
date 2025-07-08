use futures::{SinkExt as _, StreamExt as _};
use kanal::bounded_async;
use mqtt_codec_kit::{
    common::{
        Encodable, MATCH_ALL_STR, MATCH_ONE_STR, QualityOfService, SHARED_PREFIX, SYS_PREFIX,
    },
    v5::{
        control::{
            ConnackProperties, ConnectReasonCode, DisconnectProperties, DisconnectReasonCode,
        },
        packet::{
            ConnackPacket, DisconnectPacket, MqttDecoder, MqttEncoder, PublishPacket,
            VariablePacket,
        },
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

        // TODO: handle auth
        let (assigned_client_id, client_id) = if packet.client_identifier().is_empty() {
            (true, nanoid!())
        } else {
            (false, packet.client_identifier().to_owned())
        };

        // TODO: config: receive_maximum
        let mut session = Session::new(&client_id, assigned_client_id, 12);
        session.set_clean_session(packet.clean_session());
        session.set_username(packet.username().map(|name| name.to_owned()));
        session.set_keep_alive(packet.keep_alive());
        let server_keep_alive = session.keep_alive() != packet.keep_alive();
        session.set_server_keep_alive(server_keep_alive);

        let properties = packet.properties();
        if let Some(request_problem_info) = properties.request_problem_info() {
            session.set_request_problem_info(request_problem_info != 0);
        }

        if let Some(max_packet_size) = properties.max_packet_size() {
            session.set_max_packet_size(max_packet_size);
        }

        if properties.receive_maximum() == Some(0) {
            debug!("connect properties ReceiveMaximum is 0");

            let _ = frame_writer
                .send(ConnackPacket::new(false, ConnectReasonCode::QuotaExceeded))
                .await;
            return;
        }

        if session.max_packet_size() == 0 {
            debug!("connect properties MaximumPacketSize is 0");

            let _ = frame_writer
                .send(ConnackPacket::new(
                    false,
                    ConnectReasonCode::ImplementationSpecificError,
                ))
                .await;
            return;
        }

        if properties.authentication_data().is_some()
            && properties.authentication_method().is_none()
        {
            debug!("connect properties AuthenticationMethod is missing");

            let _ = frame_writer
                .send(ConnackPacket::new(
                    false,
                    ConnectReasonCode::BadAuthenticationMethod,
                ))
                .await;
            return;
        }

        if let Some(session_expiry_interval) = properties.session_expiry_interval() {
            session.set_session_expiry_interval(session_expiry_interval);
        }
        if let Some(receive_maximum) = properties.receive_maximum() {
            session.set_receive_maximum(receive_maximum);
        }
        if let Some(topic_alias_max) = properties.topic_alias_max() {
            session.set_topic_alias_max(topic_alias_max);
        }
        if let Some(request_response_info) = properties.request_response_info() {
            session.set_request_response_info(request_response_info != 0);
        }
        if !properties.user_properties().is_empty() {
            session.set_user_properties(properties.user_properties().to_vec());
        }
        if let Some(authentication_method) = properties.authentication_method() {
            session.set_authentication_method(authentication_method);
        }

        if let Some(last_will) = packet.will() {
            let topic_name = last_will.topic();
            if topic_name.is_empty()
                || topic_name.contains(MATCH_ALL_STR)
                || topic_name.contains(MATCH_ONE_STR)
                || topic_name.starts_with(SHARED_PREFIX)
                || topic_name.starts_with(SYS_PREFIX)
            {
                debug!("last will topic is empty");

                let _ = frame_writer
                    .send(ConnackPacket::new(
                        false,
                        ConnectReasonCode::TopicNameInvalid,
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
                    #[cfg(feature = "v4")]
                    ProtocolSessionState::V4(session_state) => {
                        for topic in session_state.subscriptions() {
                            if let Err(err) =
                                self.global.storage.unsubscribe(client_id, topic).await
                            {
                                debug!("handle connect unsubscribe old topic failed: {err}");
                            }
                        }

                        false
                    }

                    ProtocolSessionState::V5(session_state) => {
                        if !session.clean_session() {
                            session.copy_state(session_state);
                            true
                        } else {
                            debug!(
                                "packet id#{} session removed due to reconnect with clean session",
                                packet.client_identifier(),
                            );

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
            }
            AddClientReceipt::New => false,
        };

        // build and send connack packet
        let mut connack_properties = ConnackProperties::default();
        // TODO: config: max session_expiry_interval
        connack_properties.set_session_expiry_interval(Some(session.session_expiry_interval()));
        // TODO: config: max receive_maximum
        connack_properties.set_receive_maximum(Some(session.receive_maximum()));
        // TODO: config: max qos
        connack_properties.set_max_qos(Some(QualityOfService::Level2 as u8));
        // TODO: config: retain available
        connack_properties.set_retain_available(Some(1));
        // TODO: config: max packet size
        connack_properties.set_max_packet_size(Some(session.max_packet_size()));
        if session.assigned_client_id() {
            connack_properties
                .set_assigned_client_identifier(Some(session.client_id().to_string()));
        }
        // TODO: config: max topic alias num
        connack_properties.set_topic_alias_max(Some(session.topic_alias_max()));
        // TODO: config: wildcard_subscription_available
        connack_properties.set_wildcard_subscription_available(Some(1));
        // TODO: config: subscription_identifiers_available
        connack_properties.set_subscription_identifiers_available(Some(1));
        // TODO: config: shared_subscription_available
        // BUG: publish or subscribe QoS1/2 connect ack failed？
        // connack_properties.set_shared_subscription_available(Some(1));

        // TODO: config: min/max keep alive
        if session.server_keep_alive() {
            connack_properties.set_server_keep_alive(Some(session.keep_alive()));
        }
        if session.request_response_info() {
            // TODO: handle ResponseTopic in plugin
        }
        let mut connack_packet = ConnackPacket::new(session_present, ConnectReasonCode::Success);
        connack_packet.set_properties(connack_properties);

        if let Err(err) = frame_writer.send(connack_packet).await {
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
    fn from(msg: PendingPublishMessage) -> Self {
        let mut pkt = PublishPacket::new(
            msg.message().topic_name().to_owned(),
            msg.qos(),
            msg.message().payload(),
        );
        pkt.set_dup(msg.dup());
        pkt.set_retain(msg.message().retain());

        if let Some(p) = msg.message().properties() {
            pkt.set_properties(p.to_owned());
        }

        pkt
    }
}

pub(crate) fn build_error_disconnect<S: Into<String>>(
    session: &Session,
    reason_code: DisconnectReasonCode,
    reason_string: S,
) -> DisconnectPacket {
    let mut disconnect_packet = DisconnectPacket::new(reason_code);

    if session.request_problem_info() {
        let mut disconnect_properties = DisconnectProperties::default();
        disconnect_properties.set_reason_string(Some(reason_string.into()));
        disconnect_packet.set_properties(disconnect_properties);
    }

    if disconnect_packet.encoded_length() > session.max_packet_size() {
        disconnect_packet.set_properties(DisconnectProperties::default());
    }

    disconnect_packet
}
