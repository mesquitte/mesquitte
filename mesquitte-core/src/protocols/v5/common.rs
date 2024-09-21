use std::cmp;

use mqtt_codec_kit::{
    common::{qos::QoSWithPacketIdentifier, Encodable, QualityOfService},
    v5::{
        control::{
            ConnackProperties, ConnectReasonCode, DisconnectProperties, DisconnectReasonCode,
        },
        packet::{ConnackPacket, DisconnectPacket, PublishPacket},
    },
};

use crate::types::{publish::PublishMessage, session::Session};

fn new_publish_packet(
    session: &mut Session,
    subscribe_qos: QualityOfService,
    msg: PublishMessage,
) -> (Option<u16>, PublishPacket) {
    let final_qos = cmp::min(subscribe_qos, msg.qos());
    let (packet_id, qos) = match final_qos {
        QualityOfService::Level0 => (None, QoSWithPacketIdentifier::Level0),
        QualityOfService::Level1 => {
            let pid = session.incr_server_packet_id();
            (Some(pid), QoSWithPacketIdentifier::Level1(pid))
        }
        QualityOfService::Level2 => {
            let pid = session.incr_server_packet_id();
            (Some(pid), QoSWithPacketIdentifier::Level2(pid))
        }
    };
    let mut packet = PublishPacket::new(msg.topic_name().to_owned(), qos, msg.payload());
    packet.set_retain(msg.retain());
    packet.set_dup(msg.dup());
    if let Some(properties) = msg.properties() {
        packet.set_properties(properties.to_owned());
    }

    (packet_id, packet)
}

pub(crate) fn build_error_connack<S: Into<String>>(
    session: &mut Session,
    session_present: bool,
    reason_code: ConnectReasonCode,
    reason_string: S,
) -> ConnackPacket {
    session.set_client_disconnected();
    session.set_server_disconnected();

    let mut connack_packet = ConnackPacket::new(session_present, reason_code);

    if session.request_problem_info() {
        let mut connack_properties = ConnackProperties::default();
        connack_properties.set_reason_string(Some(reason_string.into()));
        connack_packet.set_properties(connack_properties);
    }

    if connack_packet.encoded_length() > session.max_packet_size() {
        connack_packet.set_properties(ConnackProperties::default());
    }

    connack_packet
}

pub(crate) fn build_error_disconnect<S: Into<String>>(
    session: &mut Session,
    reason_code: DisconnectReasonCode,
    reason_string: S,
) -> DisconnectPacket {
    session.set_server_disconnected();

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
