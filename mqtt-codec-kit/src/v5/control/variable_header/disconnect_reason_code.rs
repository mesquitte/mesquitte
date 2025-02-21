//! Disconnect Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `DISCONNECT` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum DisconnectReasonCode {
    /// Close the connection normally. Do not send the Will Message.
    NormalDisconnection,
    /// The Client wishes to disconnect but requires that the Server also publishes its Will Message.
    DisconnectWithWillMessage,
    /// The Connection is closed but the sender either does not wish to reveal the reason, or none of the other Reason Codes apply.
    UnspecifiedError,
    /// The received packet does not conform to this specification.
    MalformedPacket,
    /// An unexpected or out of order packet was received.
    ProtocolError,
    /// The packet received is valid but cannot be processed by this implementation.
    ImplementationSpecificError,
    /// The request is not authorized.
    NotAuthorized,
    /// The Server is busy and cannot continue processing requests from this Client.
    ServerBusy,
    /// The Server is shutting down.
    ServerShuttingDown,
    /// The Connection is closed because no packet has been received for 1.5 times the Keepalive time.
    KeepAliveTimeout,
    /// Another Connection using the same ClientID has connected causing this Connection to be closed.
    SessionTakenOver,
    /// The Topic Filter is correctly formed, but is not accepted by this Sever.
    TopicFilterInvalid,
    /// The Topic Name is correctly formed, but is not accepted by this Client or Server.
    TopicNameInvalid,
    /// The Client or Server has received more than Receive Maximum publication for which it has not sent PUBACK or PUBCOMP.
    ReceiveMaximumExceeded,
    /// The Client or Server has received a PUBLISH packet containing a Topic Alias which is greater than the Maximum Topic Alias it sent in the CONNECT or CONNACK packet.
    TopicAliasInvalid,
    /// The packet size is greater than Maximum Packet Size for this Client or Server.
    PacketTooLarge,
    /// The received data rate is too high.
    MessageRateTooHigh,
    /// An implementation or administrative imposed limit has been exceeded.
    QuotaExceeded,
    /// The Connection is closed due to an administrative action.
    AdministrativeAction,
    /// The payload format does not match the one specified by the Payload Format Indicator.
    PayloadFormatInvalid,
    /// The Server has does not support retained messages.
    RetainNotSupported,
    /// The Client specified a QoS greater than the QoS specified in a Maximum QoS in the CONNACK.
    QoSNotSupported,
    /// The Client should temporarily change its Server.
    UseAnotherServer,
    /// The Server is moved and the Client should permanently change its server location.
    ServerMoved,
    /// The Server does not support Shared Subscriptions.
    SharedSubscriptionNotSupported,
    /// This connection is closed because the connection rate is too high.
    ConnectionRateExceeded,
    /// The maximum connection time authorized for this connection has been exceeded.
    MaximumConnectTime,
    /// The Server does not support Subscription Identifiers; the subscription is not accepted.
    SubscriptionIdentifiersNotSupported,
    /// The Server does not support Wildcard subscription; the subscription is not accepted.
    WildcardSubscriptionsNotSupported,
}

impl From<DisconnectReasonCode> for u8 {
    fn from(value: DisconnectReasonCode) -> Self {
        match value {
            DisconnectReasonCode::NormalDisconnection => NORMAL_DISCONNECTION,
            DisconnectReasonCode::DisconnectWithWillMessage => DISCONNECT_WITH_WILL_MESSAGE,
            DisconnectReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            DisconnectReasonCode::MalformedPacket => MALFORMED_PACKET,
            DisconnectReasonCode::ProtocolError => PROTOCOL_ERROR,
            DisconnectReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            DisconnectReasonCode::NotAuthorized => NOT_AUTHORIZED,
            DisconnectReasonCode::ServerBusy => SERVER_BUSY,
            DisconnectReasonCode::ServerShuttingDown => SERVER_SHUTTING_DOWN,
            DisconnectReasonCode::KeepAliveTimeout => KEEP_ALIVE_TIMEOUT,
            DisconnectReasonCode::SessionTakenOver => SESSION_TAKEN_OVER,
            DisconnectReasonCode::TopicFilterInvalid => TOPIC_FILTER_INVALID,
            DisconnectReasonCode::TopicNameInvalid => TOPIC_NAME_INVALID,
            DisconnectReasonCode::ReceiveMaximumExceeded => RECEIVE_MAXIMUM_EXCEEDED,
            DisconnectReasonCode::TopicAliasInvalid => TOPIC_ALIAS_INVALID,
            DisconnectReasonCode::PacketTooLarge => PACKET_TOO_LARGE,
            DisconnectReasonCode::MessageRateTooHigh => MESSAGE_RATE_TOO_HIGH,
            DisconnectReasonCode::QuotaExceeded => QUOTA_EXCEEDED,
            DisconnectReasonCode::AdministrativeAction => ADMINISTRATIVE_ACTION,
            DisconnectReasonCode::PayloadFormatInvalid => PAYLOAD_FORMAT_INVALID,
            DisconnectReasonCode::RetainNotSupported => RETAIN_NOT_SUPPORTED,
            DisconnectReasonCode::QoSNotSupported => QOS_NOT_SUPPORTED,
            DisconnectReasonCode::UseAnotherServer => USE_ANOTHER_SERVER,
            DisconnectReasonCode::ServerMoved => SERVER_MOVED,
            DisconnectReasonCode::SharedSubscriptionNotSupported => {
                SHARED_SUBSCRIPTION_NOT_SUPPORTED
            }
            DisconnectReasonCode::ConnectionRateExceeded => CONNECTION_RATE_EXCEEDED,
            DisconnectReasonCode::MaximumConnectTime => MAXIMUM_CONNECT_TIME,
            DisconnectReasonCode::SubscriptionIdentifiersNotSupported => {
                SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
            }
            DisconnectReasonCode::WildcardSubscriptionsNotSupported => {
                WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
            }
        }
    }
}

impl From<&DisconnectReasonCode> for u8 {
    fn from(value: &DisconnectReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `DisconnectReasonCode` from value
impl TryFrom<u8> for DisconnectReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            NORMAL_DISCONNECTION => Ok(DisconnectReasonCode::NormalDisconnection),
            DISCONNECT_WITH_WILL_MESSAGE => Ok(DisconnectReasonCode::DisconnectWithWillMessage),
            UNSPECIFIED_ERROR => Ok(DisconnectReasonCode::UnspecifiedError),
            MALFORMED_PACKET => Ok(DisconnectReasonCode::MalformedPacket),
            PROTOCOL_ERROR => Ok(DisconnectReasonCode::ProtocolError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(DisconnectReasonCode::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(DisconnectReasonCode::NotAuthorized),
            SERVER_BUSY => Ok(DisconnectReasonCode::ServerBusy),
            SERVER_SHUTTING_DOWN => Ok(DisconnectReasonCode::ServerShuttingDown),
            KEEP_ALIVE_TIMEOUT => Ok(DisconnectReasonCode::KeepAliveTimeout),
            SESSION_TAKEN_OVER => Ok(DisconnectReasonCode::SessionTakenOver),
            TOPIC_FILTER_INVALID => Ok(DisconnectReasonCode::TopicFilterInvalid),
            TOPIC_NAME_INVALID => Ok(DisconnectReasonCode::TopicNameInvalid),
            RECEIVE_MAXIMUM_EXCEEDED => Ok(DisconnectReasonCode::ReceiveMaximumExceeded),
            TOPIC_ALIAS_INVALID => Ok(DisconnectReasonCode::TopicAliasInvalid),
            PACKET_TOO_LARGE => Ok(DisconnectReasonCode::PacketTooLarge),
            MESSAGE_RATE_TOO_HIGH => Ok(DisconnectReasonCode::MessageRateTooHigh),
            QUOTA_EXCEEDED => Ok(DisconnectReasonCode::QuotaExceeded),
            ADMINISTRATIVE_ACTION => Ok(DisconnectReasonCode::AdministrativeAction),
            PAYLOAD_FORMAT_INVALID => Ok(DisconnectReasonCode::PayloadFormatInvalid),
            RETAIN_NOT_SUPPORTED => Ok(DisconnectReasonCode::RetainNotSupported),
            QOS_NOT_SUPPORTED => Ok(DisconnectReasonCode::QoSNotSupported),
            USE_ANOTHER_SERVER => Ok(DisconnectReasonCode::UseAnotherServer),
            SERVER_MOVED => Ok(DisconnectReasonCode::ServerMoved),
            SHARED_SUBSCRIPTION_NOT_SUPPORTED => {
                Ok(DisconnectReasonCode::SharedSubscriptionNotSupported)
            }
            CONNECTION_RATE_EXCEEDED => Ok(DisconnectReasonCode::ConnectionRateExceeded),
            MAXIMUM_CONNECT_TIME => Ok(DisconnectReasonCode::MaximumConnectTime),
            SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED => {
                Ok(DisconnectReasonCode::SubscriptionIdentifiersNotSupported)
            }
            WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED => {
                Ok(DisconnectReasonCode::WildcardSubscriptionsNotSupported)
            }
            v => Err(VariableHeaderError::InvalidDisconnectReasonCode(v)),
        }
    }
}

impl Encodable for DisconnectReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for DisconnectReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for DisconnectReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
