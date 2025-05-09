//! PINGREQ

use std::{fmt::Display, io::Read};

use crate::{
    common::packet::DecodablePacket,
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `PINGREQ` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PingreqPacket {
    fixed_header: FixedHeader,
}

encodable_packet!(PingreqPacket());

impl PingreqPacket {
    pub fn new() -> Self {
        Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::PingRequest), 0),
        }
    }
}

impl Default for PingreqPacket {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodablePacket for PingreqPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(_reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        Ok(Self { fixed_header })
    }
}

impl Display for PingreqPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{fixed_header: {}}}", self.fixed_header)
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::{Decodable, Encodable};

    use super::*;

    #[test]
    fn test_pingreq_packet_encode_hex() {
        let packet = PingreqPacket::new();

        let expected = b"\xc0\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_pingreq_packet_decode_hex() {
        let encoded_data = b"\xc0\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PingreqPacket::decode(&mut buf).unwrap();

        let expected = PingreqPacket::new();

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_pingreq_packet_basic() {
        let packet = PingreqPacket::new();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PingreqPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_pingreq_packet() {
        let packet = PingreqPacket::new();

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PINGREQ, remaining_length: 0}}"
        );
    }
}
