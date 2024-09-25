//! DISCONNECT

use std::io::{self, Read, Write};

use crate::{
    common::{
        packet::{DecodablePacket, EncodablePacket},
        Decodable, Encodable,
    },
    v5::{
        control::{
            ControlType, DisconnectProperties, DisconnectReasonCode, FixedHeader, PacketType,
            VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `DISCONNECT` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DisconnectPacket {
    fixed_header: FixedHeader,
    reason_code: DisconnectReasonCode,
    properties: DisconnectProperties,
}

impl DisconnectPacket {
    pub fn new(reason_code: DisconnectReasonCode) -> DisconnectPacket {
        let mut fixed_header =
            FixedHeader::new(PacketType::with_default(ControlType::Disconnect), 0);

        fixed_header.remaining_length = if reason_code == DisconnectReasonCode::NormalDisconnection
        {
            0
        } else {
            1
        };

        DisconnectPacket {
            fixed_header,
            reason_code,
            properties: DisconnectProperties::default(),
        }
    }

    pub fn set_properties(&mut self, properties: DisconnectProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    pub fn properties(&self) -> &DisconnectProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> DisconnectReasonCode {
        self.reason_code
    }

    #[inline]
    fn fix_header_remaining_len(&mut self) {
        self.fixed_header.remaining_length = self.encoded_packet_length();
    }
}

impl Default for DisconnectPacket {
    fn default() -> DisconnectPacket {
        DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection)
    }
}

impl EncodablePacket for DisconnectPacket {
    type Output = FixedHeader;

    fn fixed_header(&self) -> &Self::Output {
        &self.fixed_header
    }

    fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        if self.properties.is_empty() {
            if self.reason_code != DisconnectReasonCode::NormalDisconnection {
                self.reason_code.encode(writer)?;
            }
        } else {
            self.reason_code.encode(writer)?;
            self.properties.encode(writer)?
        }
        Ok(())
    }

    fn encoded_packet_length(&self) -> u32 {
        if !self.properties.is_empty() {
            self.reason_code.encoded_length() + self.properties.encoded_length()
        } else if self.reason_code == DisconnectReasonCode::NormalDisconnection {
            0
        } else {
            1
        }
    }
}

impl DecodablePacket for DisconnectPacket {
    type DecodePacketError = std::convert::Infallible;

    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let (reason_code, properties) = if fixed_header.remaining_length == 0 {
            (
                DisconnectReasonCode::NormalDisconnection,
                DisconnectProperties::default(),
            )
        } else if fixed_header.remaining_length == 1 {
            let reason_code: DisconnectReasonCode = Decodable::decode(reader)?;
            (reason_code, DisconnectProperties::default())
        } else {
            let reason_code: DisconnectReasonCode = Decodable::decode(reader)?;
            let properties: DisconnectProperties =
                Decodable::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
            (reason_code, properties)
        };

        Ok(DisconnectPacket {
            fixed_header,
            reason_code,
            properties,
        })
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::{common::encodable::Encodable, v5::control::variable_header::DisconnectReasonCode};

    use super::*;

    #[test]
    pub fn test_disconnect_packet_encode_hex() {
        let packet = DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection);

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

        let expected = DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_disconnect_packet_decode_hex_normal_with_length() {
        let encoded_data = b"\xe0\x02\x00\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = DisconnectPacket::decode(&mut buf).unwrap();

        let expected = DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection);

        assert_eq!(
            expected.fixed_header.packet_type,
            packet.fixed_header.packet_type
        );

        assert_eq!(expected.reason_code, packet.reason_code);
        assert_eq!(expected.properties, packet.properties);
    }

    #[test]
    pub fn test_disconnect_packet_basic() {
        let packet = DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = DisconnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_disconnect_packet_with_reason() {
        let packet = DisconnectPacket::new(DisconnectReasonCode::NotAuthorized);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = DisconnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_disconnect_packet_normal_with_properties() {
        let mut packet = DisconnectPacket::new(DisconnectReasonCode::NormalDisconnection);

        let mut properties = DisconnectProperties::default();
        properties.set_reason_string(Some("Normal Disconnection".to_string()));

        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = DisconnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_disconnect_packet_with_properties() {
        let mut packet = DisconnectPacket::new(DisconnectReasonCode::NotAuthorized);

        let mut properties = DisconnectProperties::default();
        properties.set_reason_string(Some("Not Authorized".to_string()));

        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = DisconnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
