//! Pubrec Reason Code

use std::io::{self, Read, Write};

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

impl PubrecReasonCode {
    /// Get the value
    fn to_u8(self) -> u8 {
        match self {
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

/// Create `PubrecReasonCode` from value
impl TryFrom<u8> for PubrecReasonCode {
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
            v => Err(VariableHeaderError::InvalidPubrecReasonCode(v)),
        }
    }
}

impl Encodable for PubrecReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.to_u8())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubrecReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<PubrecReasonCode, VariableHeaderError> {
        reader
            .read_u8()
            .map(PubrecReasonCode::try_from)?
            .map_err(From::from)
    }
}
