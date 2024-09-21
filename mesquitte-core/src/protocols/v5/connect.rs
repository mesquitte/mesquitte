use std::{cmp, io, sync::Arc, u32};

use futures::SinkExt as _;
use mqtt_codec_kit::{
    common::{
        ProtocolLevel, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX,
    },
    v5::{
        control::{ConnackProperties, ConnectReasonCode},
        packet::{ConnackPacket, ConnectPacket, VariablePacket},
    },
};
use nanoid::nanoid;
use tokio::{io::AsyncWrite, sync::mpsc};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    protocols::{
        common::keep_alive_timer,
        v5::{common::build_error_connack, publish::handle_will},
    },
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
    if ProtocolLevel::Version50.ne(&level) {
        log::info!("unsupported protocol level: {:?}", level);

        writer
            .send(ConnackPacket::new(false, ConnectReasonCode::UnsupportedProtocolVersion).into())
            .await?;

        return Err(Error::InvalidConnectPacket);
    }

    if packet.protocol_name().ne("MQTT") {
        log::info!("unsupported protocol name: {:?}", packet.protocol_name());
        writer
            .send(ConnackPacket::new(false, ConnectReasonCode::UnsupportedProtocolVersion).into())
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
        log::error!("client identifier is empty: {:?}", packet);
        let id = nanoid!();
        session.set_assigned_client_id();
        session.set_client_identifier(&id);
    } else {
        session.set_client_identifier(packet.client_identifier());
    }

    let properties = packet.properties();
    if let Some(request_problem_info) = properties.request_problem_info() {
        session.set_request_problem_info(request_problem_info != 0);
    }

    if let Some(max_packet_size) = properties.max_packet_size() {
        session.set_max_packet_size(max_packet_size);
    }
    if properties.receive_maximum() == Some(0) {
        log::debug!("connect properties ReceiveMaximum is 0");
        let err_pkt = build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "ReceiveMaximum value=0 is not allowed",
        );

        writer.send(err_pkt.into()).await?;
        return Err(Error::InvalidConnectPacket);
    }

    if session.max_packet_size() == 0 {
        log::debug!("connect properties MaximumPacketSize is 0");
        let err_pkt = build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "MaximumPacketSize value=0 is not allowed",
        );

        writer.send(err_pkt.into()).await?;
        return Err(Error::InvalidConnectPacket);
    }

    if properties.authentication_data().is_some() && properties.authentication_method().is_none() {
        log::debug!("connect properties AuthenticationMethod is missing");
        let err_pkt = build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "AuthenticationMethod is missing",
        );
        writer.send(err_pkt.into()).await?;
        return Err(Error::InvalidConnectPacket);
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
        if topic_name.is_empty() {
            log::debug!("last will topic is empty");

            let err_pkt = build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic is empty",
            );
            writer.send(err_pkt.into()).await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
            log::debug!("last will topic contains illegal characters '+' or '#'");
            let err_pkt = build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic contains illegal characters '+' or '#'",
            );
            writer.send(err_pkt.into()).await?;

            return Err(Error::InvalidConnectPacket);
        }

        if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
            log::debug!("last will topic start with '$SYS/' or '$share/'");
            let err_pkt = build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic start with '$SYS/' or '$share/'",
            );
            writer.send(err_pkt.into()).await?;

            return Err(Error::InvalidConnectPacket);
        }
        // TODO: config: retain available
        // if last_will.retain() && !retain_available {
        //     let err_pkt = build_error_connack(
        //         &mut session,
        //         false,
        //         ConnectReasonCode::RetainNotSupported,
        //         "",
        //     );
        //     writer.send(err_pkt.into()).await?;

        //     return Err(Error::InvalidConnectPacket);
        // }

        // TODO: config: max qos
        // if last_will.qos() > max_qos {
        //     let err_pkt = build_error_connack(
        //         &mut session,
        //         false,
        //         ConnectReasonCode::QoSNotSupported,
        //         "",
        //     );
        //     writer.send(err_pkt.into()).await?;

        //     return Err(Error::InvalidConnectPacket);
        // }

        if last_will.qos() != QualityOfService::Level0 {
            session.set_last_will(Some(last_will.into()))
        }
    }
    // TODO: v5 auth

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
        global.clone(),
    )?;
    // Build and send connack packet
    let mut connack_properties = ConnackProperties::default();
    // TODO: config: max session_expiry_interval
    connack_properties.set_session_expiry_interval(Some(u32::MAX));
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
            .set_assigned_client_identifier(Some(session.client_identifier().to_string()));
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

    writer.send(connack_packet.into()).await?;

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
            Outgoing::Publish(subscribe_qos, packet) => {
                let final_qos = cmp::min(subscribe_qos, packet.qos());
                if QualityOfService::Level2.eq(&final_qos) {
                    let pid = session.incr_server_packet_id();
                    session
                        .pending_packets()
                        .push_outgoing(pid, subscribe_qos, packet);
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
