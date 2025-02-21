//! Pubcomp Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `PUBCOMP` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum PubcompReasonCode {
    Success,
    PacketIdentifierNotFound,
}

impl From<PubcompReasonCode> for u8 {
    fn from(value: PubcompReasonCode) -> Self {
        match value {
            PubcompReasonCode::Success => SUCCESS,
            PubcompReasonCode::PacketIdentifierNotFound => PACKET_IDENTIFIER_NOT_FOUND,
        }
    }
}

impl From<&PubcompReasonCode> for u8 {
    fn from(value: &PubcompReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `PubcompReasonCode` from value
impl TryFrom<u8> for PubcompReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(PubcompReasonCode::Success),
            PACKET_IDENTIFIER_NOT_FOUND => Ok(PubcompReasonCode::PacketIdentifierNotFound),
            v => Err(VariableHeaderError::InvalidPubcompReasonCode(v)),
        }
    }
}

impl Encodable for PubcompReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubcompReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for PubcompReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
