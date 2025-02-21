//! CONNACK

use std::{fmt::Display, io::Read};

use crate::{
    common::{ConnackFlags, ConnectAckFlagsError, Decodable, packet::DecodablePacket},
    v4::{
        control::{
            ControlType, FixedHeader, PacketType, VariableHeaderError,
            variable_header::ConnectReturnCode,
        },
        packet::PacketError,
    },
};

/// `CONNACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ConnackPacket {
    fixed_header: FixedHeader,
    flags: ConnackFlags,
    return_code: ConnectReturnCode,
}

encodable_packet!(ConnackPacket(flags, return_code));

impl ConnackPacket {
    pub fn new(session_present: bool, return_code: ConnectReturnCode) -> Self {
        Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::ConnectAcknowledgement),
                2,
            ),
            flags: ConnackFlags { session_present },
            return_code,
        }
    }

    pub fn connack_flags(&self) -> ConnackFlags {
        self.flags
    }

    pub fn connect_return_code(&self) -> ConnectReturnCode {
        self.return_code
    }
}

impl DecodablePacket for ConnackPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let flags: ConnackFlags = Decodable::decode(reader).map_err(|e| match e {
            ConnectAckFlagsError::IoError(err) => VariableHeaderError::IoError(err),
            ConnectAckFlagsError::InvalidReservedFlag => VariableHeaderError::InvalidReservedFlag,
        })?;
        let return_code: ConnectReturnCode = Decodable::decode(reader)?;

        Ok(Self {
            fixed_header,
            flags,
            return_code,
        })
    }
}

impl Display for ConnackPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, flags: {}, return_code: {}}}",
            self.fixed_header, self.flags, self.return_code
        )
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::{
        common::encodable::{Decodable, Encodable},
        v4::control::variable_header::ConnectReturnCode,
    };

    use super::*;

    #[test]
    fn test_connack_packet_encode_hex() {
        let packet = ConnackPacket::new(true, ConnectReturnCode::ConnectionAccepted);

        let expected = b"\x20\x02\x01\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_connack_packet_decode_hex() {
        let encoded_data = b"\x20\x02\x01\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = ConnackPacket::decode(&mut buf).unwrap();

        let expected = ConnackPacket::new(true, ConnectReturnCode::ConnectionAccepted);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_connack_packet_basic() {
        let packet = ConnackPacket::new(false, ConnectReturnCode::IdentifierRejected);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = ConnackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_connack_packet() {
        let packet = ConnackPacket::new(true, ConnectReturnCode::ConnectionAccepted);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: CONNACK, remaining_length: 2}, flags: {session_present: true}, return_code: 0}"
        );
    }
}
