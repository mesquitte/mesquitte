//! Connect Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

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

impl From<ConnectReasonCode> for u8 {
    fn from(value: ConnectReasonCode) -> Self {
        match value {
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

impl From<&ConnectReasonCode> for u8 {
    fn from(value: &ConnectReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `ConnectReasonCode` from value
impl TryFrom<u8> for ConnectReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(ConnectReasonCode::Success),
            UNSPECIFIED_ERROR => Ok(ConnectReasonCode::UnspecifiedError),
            MALFORMED_PACKET => Ok(ConnectReasonCode::MalformedPacket),
            PROTOCOL_ERROR => Ok(ConnectReasonCode::ProtocolError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(ConnectReasonCode::ImplementationSpecificError),
            UNSUPPORTED_PROTOCOL_VERSION => Ok(ConnectReasonCode::UnsupportedProtocolVersion),
            CLIENT_IDENTIFIER_NOT_VALID => Ok(ConnectReasonCode::ClientIdentifierNotValid),
            BAD_USERNAME_OR_PASSWORD => Ok(ConnectReasonCode::BadUsernameOrPassword),
            NOT_AUTHORIZED => Ok(ConnectReasonCode::NotAuthorized),
            SERVER_UNAVAILABLE => Ok(ConnectReasonCode::ServerUnavailable),
            SERVER_BUSY => Ok(ConnectReasonCode::ServerBusy),
            BANNED => Ok(ConnectReasonCode::Banned),
            BAD_AUTHENTICATION_METHOD => Ok(ConnectReasonCode::BadAuthenticationMethod),
            TOPIC_NAME_INVALID => Ok(ConnectReasonCode::TopicNameInvalid),
            PACKET_TOO_LARGE => Ok(ConnectReasonCode::PacketTooLarge),
            QUOTA_EXCEEDED => Ok(ConnectReasonCode::QuotaExceeded),
            PAYLOAD_FORMAT_INVALID => Ok(ConnectReasonCode::PayloadFormatInvalid),
            RETAIN_NOT_SUPPORTED => Ok(ConnectReasonCode::RetainNotSupported),
            QOS_NOT_SUPPORTED => Ok(ConnectReasonCode::QoSNotSupported),
            USE_ANOTHER_SERVER => Ok(ConnectReasonCode::UseAnotherServer),
            SERVER_MOVED => Ok(ConnectReasonCode::ServerMoved),
            CONNECTION_RATE_EXCEEDED => Ok(ConnectReasonCode::ConnectionRateExceeded),
            v => Err(VariableHeaderError::InvalidConnectReasonCode(v)),
        }
    }
}

impl Encodable for ConnectReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ConnectReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for ConnectReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
