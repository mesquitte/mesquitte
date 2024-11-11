use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::common::{Decodable, Encodable};

/// Packet identifier
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct PacketIdentifier(pub u16);

impl Encodable for PacketIdentifier {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u16::<BigEndian>(self.0)
    }

    fn encoded_length(&self) -> u32 {
        2
    }
}

impl Decodable for PacketIdentifier {
    type Error = io::Error;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u16::<BigEndian>().map(Self).map_err(From::from)
    }
}

impl Display for PacketIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
