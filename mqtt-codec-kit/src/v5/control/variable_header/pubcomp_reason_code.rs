//! Pubcomp Reason Code

use std::io::{self, Read, Write};

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

impl PubcompReasonCode {
    /// Get the value
    fn to_u8(self) -> u8 {
        match self {
            PubcompReasonCode::Success => SUCCESS,
            PubcompReasonCode::PacketIdentifierNotFound => PACKET_IDENTIFIER_NOT_FOUND,
        }
    }
}

/// Create `PubcompReasonCode` from value
impl TryFrom<u8> for PubcompReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(Self::Success),
            PACKET_IDENTIFIER_NOT_FOUND => Ok(Self::PacketIdentifierNotFound),
            v => Err(VariableHeaderError::InvalidPubcompReasonCode(v)),
        }
    }
}

impl Encodable for PubcompReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.to_u8())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for PubcompReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<PubcompReasonCode, VariableHeaderError> {
        reader
            .read_u8()
            .map(PubcompReasonCode::try_from)?
            .map_err(From::from)
    }
}
