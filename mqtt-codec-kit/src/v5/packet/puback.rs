//! PUBACK

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use crate::{
    common::{
        Decodable, Encodable, PacketIdentifier,
        packet::{DecodablePacket, EncodablePacket},
    },
    v5::{
        control::{
            ControlType, FixedHeader, PacketType, PubackProperties, PubackReasonCode,
            VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `PUBACK` packet, for QoS 1 delivery
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    reason_code: PubackReasonCode,
    properties: PubackProperties,
}

impl PubackPacket {
    pub fn new(pkid: u16, reason_code: PubackReasonCode) -> Self {
        let mut fixed_header = FixedHeader::new(
            PacketType::with_default(ControlType::PublishAcknowledgement),
            0,
        );

        fixed_header.remaining_length = if reason_code == PubackReasonCode::Success {
            2
        } else {
            3
        };

        Self {
            fixed_header,
            packet_identifier: PacketIdentifier(pkid),
            reason_code,
            properties: PubackProperties::default(),
        }
    }

    pub fn new_success(pkid: u16) -> Self {
        Self::new(pkid, PubackReasonCode::Success)
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn properties(&self) -> &PubackProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> PubackReasonCode {
        self.reason_code
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: PubackProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    #[inline]
    fn fix_header_remaining_len(&mut self) {
        self.fixed_header.remaining_length = self.encoded_packet_length();
    }
}

impl EncodablePacket for PubackPacket {
    type Output = FixedHeader;

    fn fixed_header(&self) -> &Self::Output {
        &self.fixed_header
    }

    fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.packet_identifier.encode(writer)?;
        if self.reason_code != PubackReasonCode::Success {
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
            if self.reason_code != PubackReasonCode::Success {
                len += self.reason_code.encoded_length();
            }
        } else {
            len += self.reason_code.encoded_length() + self.properties.encoded_length()
        }
        len
    }
}

impl DecodablePacket for PubackPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;

        let (reason_code, properties) = if fixed_header.remaining_length == 2 {
            (PubackReasonCode::Success, PubackProperties::default())
        } else if fixed_header.remaining_length == 3 {
            let reason_code = PubackReasonCode::decode(reader)?;
            (reason_code, PubackProperties::default())
        } else {
            let reason_code = PubackReasonCode::decode(reader)?;
            let properties =
                PubackProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
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

impl Display for PubackPacket {
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
    fn test_puback_packet_encode_hex() {
        let packet = PubackPacket::new(26373, PubackReasonCode::NoMatchingSubscribers);

        let expected = b"\x40\x03\x67\x05\x10";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_puback_packet_decode_hex() {
        let encoded_data = b"\x40\x02\x89\x05";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubackPacket::decode(&mut buf).unwrap();

        let expected = PubackPacket::new(35077, PubackReasonCode::Success);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_puback_packet_basic() {
        let packet = PubackPacket::new_success(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_puback_packet_with_reason() {
        let packet = PubackPacket::new(10001, PubackReasonCode::NotAuthorized);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_puback_packet_with_properties() {
        let mut packet = PubackPacket::new(10001, PubackReasonCode::NotAuthorized);

        let mut properties = PubackProperties::default();
        properties.set_reason_string(Some("Not Authorized".to_string()));
        properties.add_user_property("foo", "bar");

        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_puback_packet() {
        let packet = PubackPacket::new(123, PubackReasonCode::PacketIdentifierInUse);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBACK, remaining_length: 3}, packet_identifier: 123, reason_code: 145, properties: {reason_string: None, user_properties: []}}"
        );
    }
}
