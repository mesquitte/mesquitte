//! PINGRESP

use std::{fmt::Display, io::Read};

use crate::{
    common::packet::DecodablePacket,
    v5::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `PINGRESP` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PingrespPacket {
    fixed_header: FixedHeader,
}

encodable_packet!(PingrespPacket());

impl PingrespPacket {
    pub fn new() -> Self {
        Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::PingResponse), 0),
        }
    }
}

impl Default for PingrespPacket {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodablePacket for PingrespPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(_reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        Ok(Self { fixed_header })
    }
}

impl Display for PingrespPacket {
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
    fn test_pingresp_packet_encode_hex() {
        let packet = PingrespPacket::new();

        let expected = b"\xd0\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_pingresp_packet_decode_hex() {
        let encoded_data = b"\xd0\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PingrespPacket::decode(&mut buf).unwrap();

        let expected = PingrespPacket::new();

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_pingresp_packet_basic() {
        let packet = PingrespPacket::new();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PingrespPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_pingresp_packet() {
        let packet = PingrespPacket::new();

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PINGRESP, remaining_length: 0}}"
        );
    }
}
