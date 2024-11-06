//! DISCONNECT

use std::io::Read;

use crate::{
    common::packet::DecodablePacket,
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `DISCONNECT` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DisconnectPacket {
    fixed_header: FixedHeader,
}

encodable_packet!(DisconnectPacket());

impl DisconnectPacket {
    pub fn new() -> Self {
        Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Disconnect), 0),
        }
    }
}

impl Default for DisconnectPacket {
    fn default() -> Self {
        Self::new()
    }
}

impl DecodablePacket for DisconnectPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(_reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        Ok(Self { fixed_header })
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::{Decodable, Encodable};

    use super::*;

    #[test]
    pub fn test_disconnect_packet_encode_hex() {
        let packet = DisconnectPacket::new();

        let expected = b"\xe0\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_disconnect_packet_decode_hex() {
        let encoded_data = b"\xe0\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = DisconnectPacket::decode(&mut buf).unwrap();

        let expected = DisconnectPacket::new();

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_disconnect_packet_basic() {
        let packet = DisconnectPacket::new();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = DisconnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
