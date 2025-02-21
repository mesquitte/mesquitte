//! Pubrec Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `PUBREC` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum PubrecReasonCode {
    Success,
    NoMatchingSubscribers,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicNameInvalid,
    PacketIdentifierInUse,
    QuotaExceeded,
    PayloadFormatInvalid,
}

impl From<PubrecReasonCode> for u8 {
    fn from(value: PubrecReasonCode) -> Self {
        match value {
            PubrecReasonCode::Success => SUCCESS,
            PubrecReasonCode::NoMatchingSubscribers => NO_MATCHING_SUBSCRIBERS,
            PubrecReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            PubrecReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            PubrecReasonCode::NotAuthorized => NOT_AUTHORIZED,
            PubrecReasonCode::TopicNameInvalid => TOPIC_NAME_INVALID,
            PubrecReasonCode::PacketIdentifierInUse => PACKET_IDENTIFIER_IN_USE,
            PubrecReasonCode::QuotaExceeded => QUOTA_EXCEEDED,
            PubrecReasonCode::PayloadFormatInvalid => PAYLOAD_FORMAT_INVALID,
        }
    }
}

impl From<&PubrecReasonCode> for u8 {
    fn from(value: &PubrecReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `PubrecReasonCode` from value
impl TryFrom<u8> for PubrecReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(PubrecReasonCode::Success),
            NO_MATCHING_SUBSCRIBERS => Ok(PubrecReasonCode::NoMatchingSubscribers),
            UNSPECIFIED_ERROR => Ok(PubrecReasonCode::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(PubrecReasonCode::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(PubrecReasonCode::NotAuthorized),
            TOPIC_NAME_INVALID => Ok(PubrecReasonCode::TopicNameInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(PubrecReasonCode::PacketIdentifierInUse),
            QUOTA_EXCEEDED => Ok(PubrecReasonCode::QuotaExceeded),
            PAYLOAD_FORMAT_INVALID => Ok(PubrecReasonCode::PayloadFormatInvalid),
            v => Err(VariableHeaderError::InvalidPubrecReasonCode(v)),
        }
    }
}

impl Encodable for PubrecReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubrecReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for PubrecReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
