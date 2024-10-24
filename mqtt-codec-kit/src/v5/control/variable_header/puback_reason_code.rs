//! Puback Reason Code

use std::io::{self, Read, Write};

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
            SUCCESS => Ok(Self::Success),
            NO_MATCHING_SUBSCRIBERS => Ok(Self::NoMatchingSubscribers),
            UNSPECIFIED_ERROR => Ok(Self::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(Self::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(Self::NotAuthorized),
            TOPIC_NAME_INVALID => Ok(Self::TopicNameInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(Self::PacketIdentifierInUse),
            QUOTA_EXCEEDED => Ok(Self::QuotaExceeded),
            PAYLOAD_FORMAT_INVALID => Ok(Self::PayloadFormatInvalid),
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

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<PubackReasonCode, VariableHeaderError> {
        reader
            .read_u8()
            .map(PubackReasonCode::try_from)?
            .map_err(From::from)
    }
}
