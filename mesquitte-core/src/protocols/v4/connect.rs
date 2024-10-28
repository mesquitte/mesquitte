use kanal::{bounded_async, AsyncReceiver};
use mqtt_codec_kit::{
    common::{ProtocolLevel, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX},
    v4::{
        control::ConnectReturnCode,
        packet::{ConnackPacket, ConnectPacket},
    },
};
use nanoid::nanoid;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{
    debug, info,
    server::state::{AddClientReceipt, DeliverMessage, GlobalState},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
};

use super::{session::Session, EventLoop};

impl<R, W, S> EventLoop<R, W, S>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub async fn handle_connect<'a>(
        packet: &ConnectPacket,
        global: &'a GlobalState,
    ) -> Result<(ConnackPacket, Session, AsyncReceiver<DeliverMessage>), ConnackPacket> {
        debug!(
            r#"client#{} received a connect packet:
protocol level : {:?}
 protocol name : {:?}
 clean session : {}
      username : {:?}
      password : {:?}
    keep-alive : {}s
          will : {:?}"#,
            packet.client_identifier(),
            packet.protocol_level(),
            packet.protocol_name(),
            packet.clean_session(),
            packet.username(),
            packet.password(),
            packet.keep_alive(),
            packet.will(),
        );

        if packet.protocol_level() != ProtocolLevel::Version311 || packet.protocol_name() != "MQTT"
        {
            debug!(
                "unsupported protocol name or level: {:?} {:?}",
                packet.protocol_name(),
                packet.protocol_level()
            );

            return Err(ConnackPacket::new(
                false,
                ConnectReturnCode::UnacceptableProtocolVersion,
            ));
        }

        if packet.client_identifier().is_empty() && !packet.clean_session() {
            return Err(ConnackPacket::new(
                false,
                ConnectReturnCode::IdentifierRejected,
            ));
        }

        // TODO: handle auth

        let (assigned_client_id, client_id) = if packet.client_identifier().is_empty() {
            (true, nanoid!())
        } else {
            (false, packet.client_identifier().to_string())
        };

        let mut session = Session::new(&client_id, assigned_client_id);
        session.set_clean_session(packet.clean_session());
        session.set_username(packet.username().map(|name| name.to_owned()));
        session.set_keep_alive(packet.keep_alive());

        if let Some(last_will) = packet.will() {
            let topic_name = last_will.topic();
            if topic_name.is_empty() {
                debug!("handle connect last will topic is empty");

                return Err(ConnackPacket::new(
                    false,
                    ConnectReturnCode::IdentifierRejected,
                ));
            }

            if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
                debug!("handle connect last will topic contains illegal characters '+' or '#'");

                return Err(ConnackPacket::new(
                    false,
                    ConnectReturnCode::IdentifierRejected,
                ));
            }

            if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
                debug!("handle connect last will topic start with '$SYS/' or '$share/'");

                return Err(ConnackPacket::new(
                    false,
                    ConnectReturnCode::IdentifierRejected,
                ));
            }

            session.set_last_will(last_will)
        }

        // FIXME: too many clients cause memory leak

        // TODO: deliver channel size
        let (deliver_tx, deliver_rx) = bounded_async(8);

        let receipt = global.add_client(session.client_id(), deliver_tx).await;
        let session_present = match receipt {
            AddClientReceipt::Present(server_packet_id) => {
                if !session.clean_session() {
                    session.set_server_packet_id(server_packet_id);
                    true
                } else {
                    info!(
                        "{} session removed due to reconnect with clean session",
                        packet.client_identifier(),
                    );
                    false
                }
            }
            AddClientReceipt::New => false,
        };

        Ok((
            ConnackPacket::new(session_present, ConnectReturnCode::ConnectionAccepted),
            session,
            deliver_rx,
        ))
    }
}

pub(super) async fn handle_disconnect(session: &mut Session) {
    debug!(
        "client#{} received a disconnect packet",
        session.client_id()
    );

    session.clear_last_will();
    session.set_client_disconnected();
}
