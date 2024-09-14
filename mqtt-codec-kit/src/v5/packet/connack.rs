//! CONNACK

use std::io::Read;

use crate::{
    common::{packet::DecodablePacket, ConnackFlags, ConnectAckFlagsError, Decodable},
    v5::{
        control::{
            ConnackProperties, ConnectReasonCode, ControlType, FixedHeader, PacketType,
            VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `CONNACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ConnackPacket {
    fixed_header: FixedHeader,
    flags: ConnackFlags,
    reason_code: ConnectReasonCode,
    properties: ConnackProperties,
}

encodable_packet!(ConnackPacket(flags, reason_code, properties));

impl ConnackPacket {
    pub fn new(session_present: bool, reason_code: ConnectReasonCode) -> ConnackPacket {
        ConnackPacket {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::ConnectAcknowledgement),
                3,
            ),
            flags: ConnackFlags { session_present },
            reason_code,
            properties: ConnackProperties::default(),
        }
    }

    pub fn set_properties(&mut self, properties: ConnackProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    pub fn connack_flags(&self) -> ConnackFlags {
        self.flags
    }

    pub fn connect_return_code(&self) -> ConnectReasonCode {
        self.reason_code
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
        let reason_code: ConnectReasonCode = Decodable::decode(reader)?;
        let properties: ConnackProperties =
            Decodable::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;

        Ok(ConnackPacket {
            fixed_header,
            flags,
            reason_code,
            properties,
        })
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::{common::encodable::Encodable, v5::control::variable_header::ConnectReasonCode};

    use super::*;

    #[test]
    fn test_connack_packet_encode_hex() {
        let mut packet = ConnackPacket::new(false, ConnectReasonCode::Success);

        let mut properties = ConnackProperties::default();
        properties.set_topic_alias_max(Some(10));
        properties.set_receive_maximum(Some(20));

        packet.set_properties(properties);

        let expected = b"\x20\x09\x00\x00\x06\x21\x00\x14\x22\x00\x0a";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_connack_packet_decode_hex() {
        let encoded_data = b"\x20\x09\x01\x00\x06\x22\x00\x0a\x21\x00\x14";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = ConnackPacket::decode(&mut buf).unwrap();

        let mut expected = ConnackPacket::new(true, ConnectReasonCode::Success);
        let mut properties = ConnackProperties::default();
        properties.set_topic_alias_max(Some(10));
        properties.set_receive_maximum(Some(20));

        expected.set_properties(properties);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_connack_packet_basic() {
        let packet = ConnackPacket::new(false, ConnectReasonCode::ClientIdentifierNotValid);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = ConnackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
