//! PUBREC

use std::io::Read;

use crate::{
    common::{packet::DecodablePacket, Decodable, PacketIdentifier},
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `PUBREC` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubrecPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
}

encodable_packet!(PubrecPacket(packet_identifier));

impl PubrecPacket {
    pub fn new(pkid: u16) -> PubrecPacket {
        PubrecPacket {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::PublishReceived),
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

impl DecodablePacket for PubrecPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        Ok(PubrecPacket {
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
    pub fn test_pubrec_packet_encode_hex() {
        let packet = PubrecPacket::new(40308);

        let expected = b"\x50\x02\x9d\x74";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_pubrec_packet_decode_hex() {
        let encoded_data = b"\x50\x02\x9d\x74";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubrecPacket::decode(&mut buf).unwrap();

        let expected = PubrecPacket::new(40308);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_pubrec_packet_basic() {
        let packet = PubrecPacket::new(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrecPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
