use mqtt_codec_kit::{
    common::{ProtocolLevel, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX},
    v4::{
        control::ConnectReturnCode,
        packet::{ConnackPacket, ConnectPacket},
    },
};
use nanoid::nanoid;
use tokio::sync::mpsc;

use crate::server::state::{AddClientReceipt, DispatchMessage, GlobalState};

use super::session::Session;

pub(super) async fn handle_connect(
    packet: &ConnectPacket,
    global: &'static GlobalState,
) -> Result<(ConnackPacket, Session, mpsc::Receiver<DispatchMessage>), ConnackPacket> {
    log::debug!(
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

    let level = packet.protocol_level();
    if ProtocolLevel::Version311.ne(&level) && ProtocolLevel::Version310.ne(&level) {
        log::debug!("handle connect unsupported protocol level: {:?}", level);

        return Err(ConnackPacket::new(
            false,
            ConnectReturnCode::UnacceptableProtocolVersion,
        ));
    }

    if packet.protocol_name().ne("MQTT") {
        log::debug!(
            "handle connect unsupported protocol name: {:?}",
            packet.protocol_name()
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
        (false, packet.client_identifier().to_owned())
    };

    let mut session = Session::new(client_id, assigned_client_id);
    session.set_clean_session(packet.clean_session());
    session.set_username(packet.username().map(|name| name.to_owned()));
    session.set_keep_alive(packet.keep_alive());

    if let Some(last_will) = packet.will() {
        let topic_name = last_will.topic();
        if topic_name.is_empty() {
            log::debug!("handle connect last will topic is empty");

            return Err(ConnackPacket::new(
                false,
                ConnectReturnCode::IdentifierRejected,
            ));
        }

        if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
            log::debug!("handle connect last will topic contains illegal characters '+' or '#'");

            return Err(ConnackPacket::new(
                false,
                ConnectReturnCode::IdentifierRejected,
            ));
        }

        if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
            log::debug!("handle connect last will topic start with '$SYS/' or '$share/'");

            return Err(ConnackPacket::new(
                false,
                ConnectReturnCode::IdentifierRejected,
            ));
        }

        session.set_last_will(last_will)
    }

    // FIXME: to many clients cause memory leak

    // TODO: outgoing channel size
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<DispatchMessage>(8);
    let receipt = global.add_client(session.client_id(), outgoing_tx).await;

    let session_present = match receipt {
        AddClientReceipt::Present(server_packet_id) => {
            if !session.clean_session() {
                session.set_server_packet_id(server_packet_id);
                true
            } else {
                log::info!(
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
        outgoing_rx,
    ))
}

pub(super) async fn handle_disconnect(session: &mut Session) {
    log::debug!(
        "client#{} received a disconnect packet",
        session.client_id()
    );

    session.clear_last_will();
    session.set_client_disconnected();
}
