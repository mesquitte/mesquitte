use std::sync::Arc;

use mqtt_codec_kit::v5::{
    control::DisconnectReasonCode,
    packet::{DisconnectPacket, PingrespPacket, VariablePacket},
};

use crate::{
    server::state::GlobalState,
    types::{outgoing::Outgoing, session::Session},
};

use super::{
    common::WritePacket,
    connect::handle_disconnect,
    publish::{
        handle_puback, handle_pubcomp, handle_publish, handle_pubrec, handle_pubrel,
        receive_outgoing_publish,
    },
    subscribe::{handle_subscribe, handle_unsubscribe},
};

pub(super) async fn handle_incoming(
    session: &mut Session,
    global: Arc<GlobalState>,
    packet: VariablePacket,
) -> Option<WritePacket> {
    match packet {
        VariablePacket::PingreqPacket(_packet) => {
            Some(WritePacket::Packet(PingrespPacket::new().into()))
        }
        VariablePacket::PublishPacket(packet) => {
            handle_publish(session, &packet, global.clone()).await
        }
        VariablePacket::PubrelPacket(packet) => {
            let packet = handle_pubrel(session, global.clone(), packet.packet_identifier()).await;
            Some(WritePacket::Packet(packet.into()))
        }
        VariablePacket::PubackPacket(packet) => {
            handle_puback(session, packet.packet_identifier());
            None
        }
        VariablePacket::PubrecPacket(packet) => Some(WritePacket::Packet(
            handle_pubrec(session, packet.packet_identifier()).into(),
        )),
        VariablePacket::SubscribePacket(packet) => {
            Some(handle_subscribe(session, &packet, global.clone()))
        }
        VariablePacket::PubcompPacket(packet) => {
            handle_pubcomp(session, packet.packet_identifier());
            None
        }
        VariablePacket::UnsubscribePacket(packet) => Some(WritePacket::Packet(
            handle_unsubscribe(session, &packet, global.clone()).into(),
        )),
        VariablePacket::DisconnectPacket(packet) => {
            match handle_disconnect(session, packet).await {
                Some(disconnect) => Some(WritePacket::Disconnect(disconnect)),
                None => Some(WritePacket::Stop),
            }
        }
        _ => {
            log::debug!("unsupported packet: {:?}", packet);
            Some(WritePacket::Stop)
        }
    }
}

pub(super) async fn handle_outgoing(
    session: &mut Session,
    global: Arc<GlobalState>,
    packet: Outgoing,
) -> WritePacket {
    match packet {
        Outgoing::Publish(subscribe_qos, packet) => {
            WritePacket::Packet(receive_outgoing_publish(session, subscribe_qos, packet).into())
        }
        Outgoing::Online(sender) => {
            log::debug!(
                "handle outgoing client#{} receive new client online",
                session.client_identifier(),
            );

            global.remove_client(session.client_id(), session.subscribes().keys());
            if let Err(err) = sender.send(session.into()).await {
                log::error!(
                    "handle outgoing client#{} send session state: {err}",
                    session.client_identifier(),
                );
            }
            WritePacket::Disconnect(
                DisconnectPacket::new(DisconnectReasonCode::SessionTakenOver).into(),
            )
        }
        Outgoing::Kick(reason) => {
            log::debug!(
                "handle outgoing client#{} receive kick message: {}",
                session.client_identifier(),
                reason,
            );

            global.remove_client(session.client_id(), session.subscribes().keys());
            WritePacket::Disconnect(
                DisconnectPacket::new(DisconnectReasonCode::AdministrativeAction).into(),
            )
        }
        Outgoing::SessionExpired { connected_at } => {
            log::debug!(
                "handle outgoing client#{} session expired",
                session.client_identifier(),
            );
            if session.disconnected() && session.connected_at() == &connected_at {
                global.remove_client(session.client_id(), session.subscribes().keys());
            }

            WritePacket::Disconnect(
                DisconnectPacket::new(DisconnectReasonCode::SessionTakenOver).into(),
            )
        }
    }
}
