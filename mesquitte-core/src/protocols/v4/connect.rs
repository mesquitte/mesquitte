use std::{io, sync::Arc};

use futures::SinkExt as _;
use mqtt_codec_kit::{
    common::{ProtocolLevel, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX},
    v4::{
        control::ConnectReturnCode,
        packet::{ConnackPacket, ConnectPacket, VariablePacket},
    },
};
use nanoid::nanoid;
use tokio::{io::AsyncWrite, sync::mpsc};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    server::state::GlobalState,
    types::{client_id::AddClientReceipt, error::Error, outgoing::Outgoing, session::Session},
};

pub(super) async fn handle_connect<W, E>(
    writer: &mut FramedWrite<W, E>,
    packet: ConnectPacket,
    global: Arc<GlobalState>,
) -> Result<(Session, mpsc::Receiver<Outgoing>), Error>
where
    W: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
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
        writer
            .send(ConnackPacket::new(false, ConnectReturnCode::UnacceptableProtocolVersion).into())
            .await?;

        return Err(Error::InvalidConnectPacket);
    }

    if packet.protocol_name().ne("MQTT") {
        log::debug!(
            "handle connect unsupported protocol name: {:?}",
            packet.protocol_name()
        );
        writer
            .send(ConnackPacket::new(false, ConnectReturnCode::UnacceptableProtocolVersion).into())
            .await?;

        return Err(Error::InvalidConnectPacket);
    }

    if packet.client_identifier().is_empty() && !packet.clean_session() {
        writer
            .send(ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected).into())
            .await?;

        return Err(Error::InvalidConnectPacket);
    }

    // TODO: handle auth

    // TODO: config: max inflight size
    // TODO: config: max inflight message size
    // TODO: config: inflight message timeout
    // TODO: config: max packet size

    let mut session = Session::new(packet.client_identifier().to_owned(), 12, 1024, 10);
    session.set_clean_session(packet.clean_session());
    session.set_username(packet.username().map(|name| Arc::new(name.to_owned())));
    session.set_keep_alive(packet.keep_alive());
    let server_keep_alive = session.keep_alive() != packet.keep_alive();
    session.set_server_keep_alive(server_keep_alive);
    if packet.client_identifier().is_empty() {
        let id = nanoid!();
        session.set_assigned_client_id();
        session.set_client_identifier(&id);
    } else {
        session.set_client_identifier(packet.client_identifier());
    }

    if let Some(last_will) = packet.will() {
        let topic_name = last_will.topic();
        if topic_name.is_empty() {
            log::debug!("handle connect last will topic is empty");
            writer
                .send(ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
            log::debug!("handle connect last will topic contains illegal characters '+' or '#'");
            writer
                .send(ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
            log::debug!("handle connect last will topic start with '$SYS/' or '$share/'");
            writer
                .send(ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        session.set_last_will(Some(last_will.into()))
    }

    // FIXME: to many clients cause memory leak

    // TODO: outgoing channel size
    let (outgoing_tx, outgoing_rx) = mpsc::channel::<Outgoing>(8);
    let receipt = global
        .add_client(session.client_identifier().as_str(), outgoing_tx)
        .await?;

    let session_present = match receipt {
        AddClientReceipt::Present(client_id, old_state) => {
            session.set_client_id(client_id);
            if !session.clean_session() {
                session.copy_from_state(old_state);
                true
            } else {
                log::info!(
                    "{} session removed due to reconnect with clean session",
                    packet.client_identifier(),
                );
                false
            }
        }
        AddClientReceipt::New(client_id) => {
            session.set_client_id(client_id);
            false
        }
    };
    writer
        .send(ConnackPacket::new(session_present, ConnectReturnCode::ConnectionAccepted).into())
        .await?;

    Ok((session, outgoing_rx))
}

pub(super) async fn handle_disconnect(session: &mut Session) {
    log::debug!(
        "client#{} received a disconnect packet",
        session.client_identifier()
    );

    session.clear_last_will();
    session.set_client_disconnected();
}
