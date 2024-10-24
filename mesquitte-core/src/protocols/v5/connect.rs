use std::sync::Arc;

use mqtt_codec_kit::{
    common::{
        ProtocolLevel, QualityOfService, MATCH_ALL_STR, MATCH_ONE_STR, SHARED_PREFIX, SYS_PREFIX,
    },
    v5::{
        control::{ConnackProperties, ConnectReasonCode, DisconnectReasonCode},
        packet::{ConnackPacket, ConnectPacket, DisconnectPacket},
    },
};
use nanoid::nanoid;
use tokio::sync::mpsc;

use crate::server::state::{AddClientReceipt, DeliverMessage, GlobalState};

use super::{common::build_error_connack, session::Session};

pub(super) async fn handle_connect(
    packet: ConnectPacket,
    global: Arc<GlobalState>,
) -> Result<(ConnackPacket, Session, mpsc::Receiver<DeliverMessage>), ConnackPacket> {
    log::debug!(
        r#"client#{} received a connect packet:
protocol level : {:?}
 protocol name : {:?}
 clean session : {}
      username : {:?}
      password : {:?}
    keep-alive : {}s
          will : {:?}
    properties : {:?}"#,
        packet.client_identifier(),
        packet.protocol_level(),
        packet.protocol_name(),
        packet.clean_session(),
        packet.username(),
        packet.password(),
        packet.keep_alive(),
        packet.will(),
        packet.properties(),
    );

    let level = packet.protocol_level();
    if ProtocolLevel::Version50.ne(&level) {
        log::info!("unsupported protocol level: {:?}", level);

        return Err(ConnackPacket::new(
            false,
            ConnectReasonCode::UnsupportedProtocolVersion,
        ));
    }

    if packet.protocol_name().ne("MQTT") {
        log::info!("unsupported protocol name: {:?}", packet.protocol_name());

        return Err(ConnackPacket::new(
            false,
            ConnectReasonCode::UnsupportedProtocolVersion,
        ));
    }

    // TODO: handle auth

    let (assigned_client_id, client_id) = if packet.client_identifier().is_empty() {
        (true, nanoid!())
    } else {
        (false, packet.client_identifier().to_owned())
    };

    // TODO: config: receive_maximum
    let mut session = Session::new(client_id, assigned_client_id, 12);
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
        log::debug!("connect properties ReceiveMaximum is 0");

        return Err(build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "ReceiveMaximum value=0 is not allowed",
        ));
    }

    if session.max_packet_size() == 0 {
        log::debug!("connect properties MaximumPacketSize is 0");

        return Err(build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "MaximumPacketSize value=0 is not allowed",
        ));
    }

    if properties.authentication_data().is_some() && properties.authentication_method().is_none() {
        log::debug!("connect properties AuthenticationMethod is missing");

        return Err(build_error_connack(
            &mut session,
            false,
            ConnectReasonCode::ProtocolError,
            "AuthenticationMethod is missing",
        ));
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

            return Err(build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic is empty",
            ));
        }

        if topic_name.contains(MATCH_ALL_STR) || topic_name.contains(MATCH_ONE_STR) {
            log::debug!("last will topic contains illegal characters '+' or '#'");

            return Err(build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic contains illegal characters '+' or '#'",
            ));
        }

        if topic_name.starts_with(SHARED_PREFIX) || topic_name.starts_with(SYS_PREFIX) {
            log::debug!("last will topic start with '$SYS/' or '$share/'");

            return Err(build_error_connack(
                &mut session,
                false,
                ConnectReasonCode::TopicNameInvalid,
                "last will topic start with '$SYS/' or '$share/'",
            ));
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

        session.set_last_will(last_will)
    }
    // TODO: v5 auth

    // FIXME: too many clients cause memory leak

    // TODO: deliver channel size
    let (deliver_tx, deliver_rx) = mpsc::channel(8);
    let receipt = global.add_client(session.client_id(), deliver_tx).await;

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
        connack_properties.set_assigned_client_identifier(Some(session.client_id().to_string()));
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

    Ok((connack_packet, session, deliver_rx))
}

pub(super) async fn handle_disconnect(
    session: &mut Session,
    packet: DisconnectPacket,
) -> Option<DisconnectPacket> {
    log::debug!(
        "client#{} received a disconnect packet",
        session.client_id()
    );

    if let Some(value) = packet.properties().session_expiry_interval() {
        session.set_session_expiry_interval(value);
        session.set_clean_session(true);
    }

    if packet.reason_code() == DisconnectReasonCode::NormalDisconnection {
        session.clear_last_will();
    }
    session.set_client_disconnected();
    None
}
