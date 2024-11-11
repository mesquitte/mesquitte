//! PUBREL

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use crate::{
    common::{
        packet::{DecodablePacket, EncodablePacket},
        Decodable, Encodable, PacketIdentifier,
    },
    v5::{
        control::{
            ControlType, FixedHeader, PacketType, PubrelProperties, PubrelReasonCode,
            VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `PUBREL` packet, for QoS 2 delivery part 2
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubrelPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    reason_code: PubrelReasonCode,
    properties: PubrelProperties,
}

impl PubrelPacket {
    pub fn new(pkid: u16, reason_code: PubrelReasonCode) -> Self {
        let mut fixed_header =
            FixedHeader::new(PacketType::with_default(ControlType::PublishRelease), 0);

        fixed_header.remaining_length = if reason_code == PubrelReasonCode::Success {
            2
        } else {
            3
        };

        Self {
            fixed_header,
            packet_identifier: PacketIdentifier(pkid),
            reason_code,
            properties: PubrelProperties::default(),
        }
    }

    pub fn new_success(pkid: u16) -> Self {
        Self::new(pkid, PubrelReasonCode::Success)
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn properties(&self) -> &PubrelProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> PubrelReasonCode {
        self.reason_code
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: PubrelProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    #[inline]
    fn fix_header_remaining_len(&mut self) {
        self.fixed_header.remaining_length = self.encoded_packet_length();
    }
}

impl EncodablePacket for PubrelPacket {
    type Output = FixedHeader;

    fn fixed_header(&self) -> &Self::Output {
        &self.fixed_header
    }

    fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.packet_identifier.encode(writer)?;
        if self.reason_code != PubrelReasonCode::Success {
            self.reason_code.encode(writer)?;
            if !self.properties.clone().is_empty() {
                self.properties.encode(writer)?
            }
        }
        Ok(())
    }

    fn encoded_packet_length(&self) -> u32 {
        let mut len = self.packet_identifier.encoded_length();
        if self.properties.is_empty() {
            if self.reason_code != PubrelReasonCode::Success {
                len += self.reason_code.encoded_length();
            }
        } else {
            len += self.reason_code.encoded_length() + self.properties.encoded_length()
        }
        len
    }
}

impl DecodablePacket for PubrelPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;

        let (reason_code, properties) = if fixed_header.remaining_length == 2 {
            (PubrelReasonCode::Success, PubrelProperties::default())
        } else if fixed_header.remaining_length == 3 {
            let reason_code = PubrelReasonCode::decode(reader)?;
            (reason_code, PubrelProperties::default())
        } else {
            let reason_code = PubrelReasonCode::decode(reader)?;
            let properties =
                PubrelProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
            (reason_code, properties)
        };

        Ok(Self {
            fixed_header,
            packet_identifier,
            reason_code,
            properties,
        })
    }
}

impl Display for PubrelPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}, reason_code: {}, properties: {}}}",
            self.fixed_header, self.packet_identifier, self.reason_code, self.properties
        )
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    fn test_pubrel_packet_encode_hex() {
        let packet = PubrelPacket::new(48059, PubrelReasonCode::Success);

        let expected = b"\x62\x02\xbb\xbb";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_pubrel_packet_decode_hex() {
        let encoded_data = b"\x62\x02\xbb\xbb";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubrelPacket::decode(&mut buf).unwrap();

        let expected = PubrelPacket::new(48059, PubrelReasonCode::Success);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_pubrel_packet_basic() {
        let packet = PubrelPacket::new_success(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrelPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_pubrel_packet_with_reason() {
        let packet = PubrelPacket::new(10001, PubrelReasonCode::PacketIdentifierNotFound);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrelPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_pubrel_packet_with_properties() {
        let mut packet = PubrelPacket::new(10001, PubrelReasonCode::PacketIdentifierNotFound);

        let mut properties = PubrelProperties::default();
        properties.set_reason_string(Some("Packet Identifier Not Found".to_string()));
        properties.add_user_property("foo", "bar");

        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrelPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_pubrel_packet() {
        let packet = PubrelPacket::new(123, PubrelReasonCode::PacketIdentifierNotFound);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBREL, remaining_length: 3}, packet_identifier: 123, reason_code: 146, properties: {reason_string: None, user_properties: []}}"
        );
    }
}
