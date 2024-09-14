use std::{
    error::Error,
    io::{self, Read, Write},
};

use super::Encodable;

/// A trait representing a packet that can be encoded, when passed as `FooPacket` or as
/// `&FooPacket`. Different from [`Encodable`] in that it prevents you from accidentally passing
/// a type intended to be encoded only as a part of a packet and doesn't have a header, e.g.
/// `Vec<u8>`.
pub trait EncodablePacket {
    type Output: Encodable;
    /// Get a reference to `FixedHeader`. All MQTT packet must have a fixed header.
    fn fixed_header(&self) -> &Self::Output;

    /// Encodes packet data after fixed header, including variable headers and payload
    fn encode_packet<W: Write>(&self, _writer: &mut W) -> io::Result<()> {
        Ok(())
    }

    /// Length in bytes for data after fixed header, including variable headers and payload
    fn encoded_packet_length(&self) -> u32 {
        0
    }
}

impl<T: EncodablePacket> Encodable for T {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.fixed_header().encode(writer)?;
        self.encode_packet(writer)
    }

    fn encoded_length(&self) -> u32 {
        self.fixed_header().encoded_length() + self.encoded_packet_length()
    }
}

pub trait DecodablePacket: EncodablePacket + Sized {
    type DecodePacketError: Error + 'static;
    type F;
    type Error;

    /// Decode packet given a `FixedHeader`
    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error>;
}
