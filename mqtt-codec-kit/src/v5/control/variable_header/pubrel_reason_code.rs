//! Pubrel Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `PUBREL` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum PubrelReasonCode {
    Success,
    PacketIdentifierNotFound,
}

impl From<PubrelReasonCode> for u8 {
    fn from(value: PubrelReasonCode) -> Self {
        match value {
            PubrelReasonCode::Success => SUCCESS,
            PubrelReasonCode::PacketIdentifierNotFound => PACKET_IDENTIFIER_NOT_FOUND,
        }
    }
}

impl From<&PubrelReasonCode> for u8 {
    fn from(value: &PubrelReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `PubrelReasonCode` from value
impl TryFrom<u8> for PubrelReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(PubrelReasonCode::Success),
            PACKET_IDENTIFIER_NOT_FOUND => Ok(PubrelReasonCode::PacketIdentifierNotFound),
            v => Err(VariableHeaderError::InvalidPubrelReasonCode(v)),
        }
    }
}

impl Encodable for PubrelReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubrelReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for PubrelReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
