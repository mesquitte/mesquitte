//! PUBREC

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
            ControlType, FixedHeader, PacketType, PubrecProperties, PubrecReasonCode,
            VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `PUBREC` packet, for QoS 2 delivery part 1
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PubrecPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    reason_code: PubrecReasonCode,
    properties: PubrecProperties,
}

impl PubrecPacket {
    pub fn new(pkid: u16, reason_code: PubrecReasonCode) -> Self {
        let mut fixed_header =
            FixedHeader::new(PacketType::with_default(ControlType::PublishReceived), 0);

        fixed_header.remaining_length = if reason_code == PubrecReasonCode::Success {
            2
        } else {
            3
        };

        Self {
            fixed_header,
            packet_identifier: PacketIdentifier(pkid),
            reason_code,
            properties: PubrecProperties::default(),
        }
    }

    pub fn new_success(pkid: u16) -> Self {
        Self::new(pkid, PubrecReasonCode::Success)
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn properties(&self) -> &PubrecProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> PubrecReasonCode {
        self.reason_code
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: PubrecProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    #[inline]
    fn fix_header_remaining_len(&mut self) {
        self.fixed_header.remaining_length = self.encoded_packet_length();
    }
}

impl EncodablePacket for PubrecPacket {
    type Output = FixedHeader;

    fn fixed_header(&self) -> &Self::Output {
        &self.fixed_header
    }

    fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.packet_identifier.encode(writer)?;
        if self.reason_code != PubrecReasonCode::Success {
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
            if self.reason_code != PubrecReasonCode::Success {
                len += self.reason_code.encoded_length();
            }
        } else {
            len += self.reason_code.encoded_length() + self.properties.encoded_length()
        }
        len
    }
}

impl DecodablePacket for PubrecPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;

        let (reason_code, properties) = if fixed_header.remaining_length == 2 {
            (PubrecReasonCode::Success, PubrecProperties::default())
        } else if fixed_header.remaining_length == 3 {
            let reason_code = PubrecReasonCode::decode(reader)?;
            (reason_code, PubrecProperties::default())
        } else {
            let reason_code = PubrecReasonCode::decode(reader)?;
            let properties =
                PubrecProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
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

impl Display for PubrecPacket {
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
    fn test_pubrec_packet_encode_hex() {
        let packet = PubrecPacket::new(48059, PubrecReasonCode::Success);

        let expected = b"\x50\x02\xbb\xbb";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_pubrec_packet_decode_hex() {
        let encoded_data = b"\x50\x02\xbb\xbb";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PubrecPacket::decode(&mut buf).unwrap();

        let expected = PubrecPacket::new(48059, PubrecReasonCode::Success);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_pubrec_packet_basic() {
        let packet = PubrecPacket::new_success(10001);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrecPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_pubrec_packet_with_reason() {
        let packet = PubrecPacket::new(10001, PubrecReasonCode::NotAuthorized);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrecPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_pubrec_packet_with_properties() {
        let mut packet = PubrecPacket::new(10001, PubrecReasonCode::NotAuthorized);

        let mut properties = PubrecProperties::default();
        properties.set_reason_string(Some("Not Authorized".to_string()));
        properties.add_user_property("foo", "bar");

        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PubrecPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_pubrec_packet() {
        let packet = PubrecPacket::new(123, PubrecReasonCode::PayloadFormatInvalid);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBREC, remaining_length: 3}, packet_identifier: 123, reason_code: 153, properties: {reason_string: None, user_properties: []}}"
        );
    }
}
