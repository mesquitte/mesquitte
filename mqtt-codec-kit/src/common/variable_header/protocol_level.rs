//! Protocol level header

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::common::{Decodable, Encodable};

pub const SPEC_3_1_0: u8 = 0x03;
pub const SPEC_3_1_1: u8 = 0x04;
pub const SPEC_5_0: u8 = 0x05;

/// Protocol level in MQTT (`0x04` in v3.1.1)
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum ProtocolLevel {
    Version310 = SPEC_3_1_0,
    Version311 = SPEC_3_1_1,
    Version50 = SPEC_5_0,
}

impl TryFrom<u8> for ProtocolLevel {
    type Error = ProtocolLevelError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SPEC_3_1_0 => Ok(ProtocolLevel::Version310),
            SPEC_3_1_1 => Ok(ProtocolLevel::Version311),
            SPEC_5_0 => Ok(ProtocolLevel::Version50),
            lvl => Err(ProtocolLevelError::InvalidProtocolLevel(lvl)),
        }
    }
}

impl Encodable for ProtocolLevel {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(*self as u8)
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ProtocolLevel {
    type Error = ProtocolLevelError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for ProtocolLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ProtocolLevelError {
    IoError(#[from] io::Error),
    #[error("invalid protocol level ({0})")]
    InvalidProtocolLevel(u8),
}
