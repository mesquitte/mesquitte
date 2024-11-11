//! Puback Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `PUBACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum PubackReasonCode {
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

impl From<PubackReasonCode> for u8 {
    fn from(value: PubackReasonCode) -> Self {
        match value {
            PubackReasonCode::Success => SUCCESS,
            PubackReasonCode::NoMatchingSubscribers => NO_MATCHING_SUBSCRIBERS,
            PubackReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            PubackReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            PubackReasonCode::NotAuthorized => NOT_AUTHORIZED,
            PubackReasonCode::TopicNameInvalid => TOPIC_NAME_INVALID,
            PubackReasonCode::PacketIdentifierInUse => PACKET_IDENTIFIER_IN_USE,
            PubackReasonCode::QuotaExceeded => QUOTA_EXCEEDED,
            PubackReasonCode::PayloadFormatInvalid => PAYLOAD_FORMAT_INVALID,
        }
    }
}

impl From<&PubackReasonCode> for u8 {
    fn from(value: &PubackReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `PubackReasonCode` from value
impl TryFrom<u8> for PubackReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(PubackReasonCode::Success),
            NO_MATCHING_SUBSCRIBERS => Ok(PubackReasonCode::NoMatchingSubscribers),
            UNSPECIFIED_ERROR => Ok(PubackReasonCode::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(PubackReasonCode::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(PubackReasonCode::NotAuthorized),
            TOPIC_NAME_INVALID => Ok(PubackReasonCode::TopicNameInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(PubackReasonCode::PacketIdentifierInUse),
            QUOTA_EXCEEDED => Ok(PubackReasonCode::QuotaExceeded),
            PAYLOAD_FORMAT_INVALID => Ok(PubackReasonCode::PayloadFormatInvalid),
            v => Err(VariableHeaderError::InvalidPubackReasonCode(v)),
        }
    }
}

impl Encodable for PubackReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubackReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?.map_err(From::from)
    }
}

impl Display for PubackReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
