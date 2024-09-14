//! UNSUBACK

use std::io::Read;

use crate::{
    common::{packet::DecodablePacket, Decodable, PacketIdentifier},
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `UNSUBACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UnsubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
}

encodable_packet!(UnsubackPacket(packet_identifier));

impl UnsubackPacket {
    pub fn new(pkid: u16) -> UnsubackPacket {
        UnsubackPacket {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::UnsubscribeAcknowledgement),
                2,
            ),
            packet_identifier: PacketIdentifier(pkid),
        }
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }
}

impl DecodablePacket for UnsubackPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        Ok(UnsubackPacket {
            fixed_header,
            packet_identifier,
        })
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    pub fn test_unsuback_packet_encode_hex() {
        let packet = UnsubackPacket::new(40304);

        let expected = b"\xb0\x02\x9d\x70";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_unsuback_packet_decode_hex() {
        let encoded_data = b"\xb0\x02\x9d\x71";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = UnsubackPacket::decode(&mut buf).unwrap();

        let expected = UnsubackPacket::new(40305);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_unsuback_packet_basic() {
        let packet = UnsubackPacket::new(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = UnsubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
