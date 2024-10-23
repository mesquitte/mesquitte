//! Connect Reason Code

use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `CONNACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ConnectReasonCode {
    Success,
    UnspecifiedError,
    MalformedPacket,
    ProtocolError,
    ImplementationSpecificError,
    UnsupportedProtocolVersion,
    ClientIdentifierNotValid,
    BadUsernameOrPassword,
    NotAuthorized,
    ServerUnavailable,
    ServerBusy,
    Banned,
    BadAuthenticationMethod,
    TopicNameInvalid,
    PacketTooLarge,
    QuotaExceeded,
    PayloadFormatInvalid,
    RetainNotSupported,
    QoSNotSupported,
    UseAnotherServer,
    ServerMoved,
    ConnectionRateExceeded,
}

impl ConnectReasonCode {
    /// Get the value
    pub fn to_u8(self) -> u8 {
        match self {
            ConnectReasonCode::Success => SUCCESS,
            ConnectReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            ConnectReasonCode::MalformedPacket => MALFORMED_PACKET,
            ConnectReasonCode::ProtocolError => PROTOCOL_ERROR,
            ConnectReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            ConnectReasonCode::UnsupportedProtocolVersion => UNSUPPORTED_PROTOCOL_VERSION,
            ConnectReasonCode::ClientIdentifierNotValid => CLIENT_IDENTIFIER_NOT_VALID,
            ConnectReasonCode::BadUsernameOrPassword => BAD_USERNAME_OR_PASSWORD,
            ConnectReasonCode::NotAuthorized => NOT_AUTHORIZED,
            ConnectReasonCode::ServerUnavailable => SERVER_UNAVAILABLE,
            ConnectReasonCode::ServerBusy => SERVER_BUSY,
            ConnectReasonCode::Banned => BANNED,
            ConnectReasonCode::BadAuthenticationMethod => BAD_AUTHENTICATION_METHOD,
            ConnectReasonCode::TopicNameInvalid => TOPIC_NAME_INVALID,
            ConnectReasonCode::PacketTooLarge => PACKET_TOO_LARGE,
            ConnectReasonCode::QuotaExceeded => QUOTA_EXCEEDED,
            ConnectReasonCode::PayloadFormatInvalid => PAYLOAD_FORMAT_INVALID,
            ConnectReasonCode::RetainNotSupported => RETAIN_NOT_SUPPORTED,
            ConnectReasonCode::QoSNotSupported => QOS_NOT_SUPPORTED,
            ConnectReasonCode::UseAnotherServer => USE_ANOTHER_SERVER,
            ConnectReasonCode::ServerMoved => SERVER_MOVED,
            ConnectReasonCode::ConnectionRateExceeded => CONNECTION_RATE_EXCEEDED,
        }
    }
}

/// Create `ConnectReasonCode` from value
impl TryFrom<u8> for ConnectReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(Self::Success),
            UNSPECIFIED_ERROR => Ok(Self::UnspecifiedError),
            MALFORMED_PACKET => Ok(Self::MalformedPacket),
            PROTOCOL_ERROR => Ok(Self::ProtocolError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(Self::ImplementationSpecificError),
            UNSUPPORTED_PROTOCOL_VERSION => Ok(Self::UnsupportedProtocolVersion),
            CLIENT_IDENTIFIER_NOT_VALID => Ok(Self::ClientIdentifierNotValid),
            BAD_USERNAME_OR_PASSWORD => Ok(Self::BadUsernameOrPassword),
            NOT_AUTHORIZED => Ok(Self::NotAuthorized),
            SERVER_UNAVAILABLE => Ok(Self::ServerUnavailable),
            SERVER_BUSY => Ok(Self::ServerBusy),
            BANNED => Ok(Self::Banned),
            BAD_AUTHENTICATION_METHOD => Ok(Self::BadAuthenticationMethod),
            TOPIC_NAME_INVALID => Ok(Self::TopicNameInvalid),
            PACKET_TOO_LARGE => Ok(Self::PacketTooLarge),
            QUOTA_EXCEEDED => Ok(Self::QuotaExceeded),
            PAYLOAD_FORMAT_INVALID => Ok(Self::PayloadFormatInvalid),
            RETAIN_NOT_SUPPORTED => Ok(Self::RetainNotSupported),
            QOS_NOT_SUPPORTED => Ok(Self::QoSNotSupported),
            USE_ANOTHER_SERVER => Ok(Self::UseAnotherServer),
            SERVER_MOVED => Ok(Self::ServerMoved),
            CONNECTION_RATE_EXCEEDED => Ok(Self::ConnectionRateExceeded),
            v => Err(VariableHeaderError::InvalidConnectReasonCode(v)),
        }
    }
}

impl Encodable for ConnectReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.to_u8())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ConnectReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<ConnectReasonCode, VariableHeaderError> {
        reader
            .read_u8()
            .map(ConnectReasonCode::try_from)?
            .map_err(From::from)
    }
}
