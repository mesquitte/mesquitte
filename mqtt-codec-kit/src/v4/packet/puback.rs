//! PUBACK

use std::{fmt::Display, io::Read};

use crate::{
    common::{packet::DecodablePacket, Decodable, PacketIdentifier},
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `PUBACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
}

encodable_packet!(PubackPacket(packet_identifier));

impl PubackPacket {
    pub fn new(pkid: u16) -> Self {
        Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::PublishAcknowledgement),
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

impl DecodablePacket for PubackPacket {
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

impl Display for PubackPacket {
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
    fn test_puback_packet_encode_hex() {
        let packet = PubackPacket::new(40306);

        let expected = b"\x40\x02\x9d\x72";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_puback_packet_decode_hex() {
        let encoded_data = b"\x40\x02\x9d\x73";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubackPacket::decode(&mut buf).unwrap();

        let expected = PubackPacket::new(40307);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_puback_packet_basic() {
        let packet = PubackPacket::new(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_puback_packet() {
        let packet = PubackPacket::new(123);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBACK, remaining_length: 2}, packet_identifier: 123}"
        );
    }
}
