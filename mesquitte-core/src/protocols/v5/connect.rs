use std::{io, sync::Arc};

use futures::SinkExt as _;
use mqtt_codec_kit::{
    common::{
        ProtocolLevel, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX,
    },
    v5::{
        control::ConnectReasonCode,
        packet::{ConnackPacket, ConnectPacket, VariablePacket},
    },
};
use nanoid::nanoid;
use tokio::{io::AsyncWrite, sync::mpsc};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    protocols::{common::keep_alive_timer, v5::publish::handle_will},
    server::state::GlobalState,
    types::{
        client_id::AddClientReceipt,
        error::Error,
        outgoing::{KickReason, Outgoing},
        session::Session,
    },
};

pub(super) async fn handle_connect<W, E>(
    writer: &mut FramedWrite<W, E>,
    packet: &ConnectPacket,
    global: Arc<GlobalState>,
) -> Result<(Session, mpsc::Receiver<Outgoing>), Error>
where
    W: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
{
    log::debug!(
        r#"client#{} received a connect packet:
protocol_level : {:?}
clean session : {}
     username : {:?}
     password : {:?}
   keep-alive : {}s
         will : {:?}"#,
        packet.client_identifier(),
        packet.protocol_level(),
        packet.clean_session(),
        packet.username(),
        packet.password(),
        packet.keep_alive(),
        packet.will(),
    );

    let level = packet.protocol_level();
    if ProtocolLevel::Version50.ne(&level) {
        log::info!("unsupported protocol level: {:?}", level);
        writer
            .send(ConnackPacket::new(false, ConnectReasonCode::UnsupportedProtocolVersion).into())
            .await?;

        return Err(Error::InvalidConnectPacket);
    }

    // TODO: handle auth

    // TODO: config: max inflight size
    // TODO: config: max inflight message size
    // TODO: config: inflight message timeout

    let mut session = Session::new(packet.client_identifier().to_owned(), 12, 1024, 10);
    session.set_clean_session(packet.clean_session());
    session.set_username(packet.username().map(|name| Arc::new(name.to_owned())));
    session.set_keep_alive(packet.keep_alive());
    let server_keep_alive = session.keep_alive() != packet.keep_alive();
    session.set_server_keep_alive(server_keep_alive);

    if packet.client_identifier().is_empty() {
        log::error!("client identifier is empty: {:?}", packet);
        let id = nanoid!();
        session.set_assigned_client_id();
        session.set_client_identifier(&id);
    } else {
        session.set_client_identifier(packet.client_identifier());
    }

    if let Some(last_will) = packet.will() {
        let topic_name = last_will.topic();
        if topic_name.is_empty() {
            log::debug!("last will topic is empty");
            writer
                .send(ConnackPacket::new(false, ConnectReasonCode::TopicNameInvalid).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
            log::debug!("last will topic contains illegal characters '+' or '#'");
            writer
                .send(ConnackPacket::new(false, ConnectReasonCode::TopicNameInvalid).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
            log::debug!("last will topic start with '$SYS/' or '$share/'");
            writer
                .send(ConnackPacket::new(false, ConnectReasonCode::TopicNameInvalid).into())
                .await?;

            return Err(Error::InvalidConnectPacket);
        }

        if last_will.retain() && last_will.qos() != QualityOfService::Level0 {
            session.set_last_will(Some(last_will.into()))
        }
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
    keep_alive_timer(
        session.keep_alive(),
        session.client_id(),
        session.last_packet_at(),
        &global,
    )?;
    writer
        .send(ConnackPacket::new(session_present, ConnectReasonCode::Success).into())
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

pub(super) async fn handle_offline(
    mut session: Session,
    mut outgoing_rx: mpsc::Receiver<Outgoing>,
    global: Arc<GlobalState>,
    take_over: bool,
) {
    log::debug!(
        r#"client#{} handle offline:
clean session : {}
    take over : {}"#,
        session.client_identifier(),
        session.clean_session(),
        take_over,
    );
    if !session.disconnected() {
        session.set_server_disconnected();
    }

    if !session.client_disconnected() {
        handle_will(&mut session, global.clone()).await;
    }

    if !take_over || session.clean_session() {
        global.remove_client(session.client_id(), session.subscribes().keys());
        return;
    }

    while let Some(outgoing) = outgoing_rx.recv().await {
        match outgoing {
            Outgoing::Publish(qos, msg) => {
                if qos == QualityOfService::Level2 {
                    let pid = session.incr_server_packet_id();
                    session.pending_packets().push_outgoing(pid, msg);
                }
            }
            Outgoing::Online(sender) => {
                log::debug!(
                    "handle offline client#{} receive new client online",
                    session.client_identifier(),
                );
                global.remove_client(session.client_id(), session.subscribes().keys());
                if let Err(err) = sender.send((&mut session).into()).await {
                    log::debug!(
                        "handle offline client#{} send session state : {}",
                        session.client_identifier(),
                        err,
                    );
                }
                break;
            }
            Outgoing::Kick(reason) => {
                log::debug!(
                    "handle offline client#{} receive kick message",
                    session.client_identifier(),
                );
                if KickReason::Expired == reason {
                    continue;
                }

                global.remove_client(session.client_id(), session.subscribes().keys());
                break;
            }
        }
    }
}
