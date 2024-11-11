//! PUBCOMP

use std::{fmt::Display, io::Read};

use crate::{
    common::{packet::DecodablePacket, Decodable, PacketIdentifier},
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `PUBCOMP` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubcompPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
}

encodable_packet!(PubcompPacket(packet_identifier));

impl PubcompPacket {
    pub fn new(pkid: u16) -> Self {
        Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::PublishComplete),
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

impl DecodablePacket for PubcompPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        Ok(Self {
            fixed_header,
            packet_identifier,
        })
    }
}

impl Display for PubcompPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}}}",
            self.fixed_header, self.packet_identifier
        )
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    fn test_pubcomp_packet_encode_hex() {
        let packet = PubcompPacket::new(40308);

        let expected = b"\x70\x02\x9d\x74";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_pubcomp_packet_decode_hex() {
        let encoded_data = b"\x70\x02\x9d\x74";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubcompPacket::decode(&mut buf).unwrap();

        let expected = PubcompPacket::new(40308);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_pubcomp_packet_basic() {
        let packet = PubcompPacket::new(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubcompPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_pubcomp_packet() {
        let packet = PubcompPacket::new(123);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBCOMP, remaining_length: 2}, packet_identifier: 123}"
        );
    }
}
