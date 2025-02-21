//! SUBACK

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable, PacketIdentifier, packet::DecodablePacket},
    v5::{
        control::{ControlType, FixedHeader, PacketType, SubackProperties, VariableHeaderError},
        packet::PacketError,
        reason_code_value::*,
    },
};

/// `SUBACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    properties: SubackProperties,
    payload: SubackPacketPayload,
}

encodable_packet!(SubackPacket(packet_identifier, properties, payload));

impl SubackPacket {
    pub fn new(pkid: u16, reason_codes: Vec<SubscribeReasonCode>) -> Self {
        let mut pkt = Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::SubscribeAcknowledgement),
                0,
            ),
            packet_identifier: PacketIdentifier(pkid),
            properties: SubackProperties::default(),
            payload: SubackPacketPayload::new(reason_codes),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn properties(&self) -> &SubackProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> &[SubscribeReasonCode] {
        &self.payload.reason_codes[..]
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: SubackProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }
}

impl DecodablePacket for SubackPacket {
    type DecodePacketError = SubackPacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier = PacketIdentifier::decode(reader)?;
        let properties =
            SubackProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
        let payload: SubackPacketPayload = SubackPacketPayload::decode_with(
            reader,
            fixed_header.remaining_length
                - packet_identifier.encoded_length()
                - properties.encoded_length(),
        )
        .map_err(PacketError::PayloadError)?;

        Ok(Self {
            fixed_header,
            packet_identifier,
            properties,
            payload,
        })
    }
}

impl Display for SubackPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}, properties: {}, payload: {}}}",
            self.fixed_header, self.packet_identifier, self.properties, self.payload
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct SubackPacketPayload {
    reason_codes: Vec<SubscribeReasonCode>,
}

impl SubackPacketPayload {
    pub fn new(reason_codes: Vec<SubscribeReasonCode>) -> Self {
        Self { reason_codes }
    }
}

impl Encodable for SubackPacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for code in self.reason_codes.iter() {
            code.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.reason_codes.len() as u32
    }
}

impl Decodable for SubackPacketPayload {
    type Error = SubackPacketError;
    type Cond = u32;

    fn decode_with<R: Read>(reader: &mut R, payload_len: u32) -> Result<Self, Self::Error> {
        let mut subs = Vec::new();

        for _ in 0..payload_len {
            subs.push(reader.read_u8()?.try_into()?);
        }

        Ok(Self::new(subs))
    }
}

impl Display for SubackPacketPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{reason_codes: [")?;
        let mut iter = self.reason_codes.iter();
        if let Some(first) = iter.next() {
            write!(f, "{}", first)?;
            for code in iter {
                write!(f, ", {}", code)?;
            }
        }
        write!(f, "]}}")
    }
}

/// Reason code for `SUBACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SubscribeReasonCode {
    GrantedQos0,
    GrantedQos1,
    GrantedQos2,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicFilterInvalid,
    PacketIdentifierInUse,
    QuotaExceeded,
    SharedSubscriptionNotSupported,
    SubscriptionIdentifiersNotSupported,
    WildcardSubscriptionsNotSupported,
}

impl From<SubscribeReasonCode> for u8 {
    fn from(value: SubscribeReasonCode) -> Self {
        match value {
            SubscribeReasonCode::GrantedQos0 => GRANTED_QOS_0,
            SubscribeReasonCode::GrantedQos1 => GRANTED_QOS_1,
            SubscribeReasonCode::GrantedQos2 => GRANTED_QOS_2,
            SubscribeReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            SubscribeReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            SubscribeReasonCode::NotAuthorized => NOT_AUTHORIZED,
            SubscribeReasonCode::TopicFilterInvalid => TOPIC_FILTER_INVALID,
            SubscribeReasonCode::PacketIdentifierInUse => PACKET_IDENTIFIER_IN_USE,
            SubscribeReasonCode::QuotaExceeded => QUOTA_EXCEEDED,
            SubscribeReasonCode::SharedSubscriptionNotSupported => {
                SHARED_SUBSCRIPTION_NOT_SUPPORTED
            }
            SubscribeReasonCode::SubscriptionIdentifiersNotSupported => {
                SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED
            }
            SubscribeReasonCode::WildcardSubscriptionsNotSupported => {
                WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED
            }
        }
    }
}

impl From<&SubscribeReasonCode> for u8 {
    fn from(value: &SubscribeReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `SubscribeReasonCode` from value
impl TryFrom<u8> for SubscribeReasonCode {
    type Error = SubackPacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            GRANTED_QOS_0 => Ok(SubscribeReasonCode::GrantedQos0),
            GRANTED_QOS_1 => Ok(SubscribeReasonCode::GrantedQos1),
            GRANTED_QOS_2 => Ok(SubscribeReasonCode::GrantedQos2),
            UNSPECIFIED_ERROR => Ok(SubscribeReasonCode::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(SubscribeReasonCode::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(SubscribeReasonCode::NotAuthorized),
            TOPIC_FILTER_INVALID => Ok(SubscribeReasonCode::TopicFilterInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(SubscribeReasonCode::PacketIdentifierInUse),
            QUOTA_EXCEEDED => Ok(SubscribeReasonCode::QuotaExceeded),
            SHARED_SUBSCRIPTION_NOT_SUPPORTED => {
                Ok(SubscribeReasonCode::SharedSubscriptionNotSupported)
            }
            SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED => {
                Ok(SubscribeReasonCode::SubscriptionIdentifiersNotSupported)
            }
            WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED => {
                Ok(SubscribeReasonCode::WildcardSubscriptionsNotSupported)
            }
            v => Err(SubackPacketError::InvalidSubscribeReasonCode(v)),
        }
    }
}

impl Encodable for SubscribeReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for SubscribeReasonCode {
    type Error = SubackPacketError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for SubscribeReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubackPacketError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid subscribe reason code ({0})")]
    InvalidSubscribeReasonCode(u8),
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    fn test_suback_packet_encode_hex() {
        let packet = SubackPacket::new(63254, vec![SubscribeReasonCode::GrantedQos0]);

        let expected = b"\x90\x04\xf7\x16\x00\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_suback_packet_decode_hex() {
        let encoded_data = b"\x90\x04\xf7\x17\x00\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = SubackPacket::decode(&mut buf).unwrap();

        let expected = SubackPacket::new(63255, vec![SubscribeReasonCode::GrantedQos0]);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_suback_packet_basic() {
        let subscribes = vec![SubscribeReasonCode::GrantedQos0];

        let packet = SubackPacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_suback_packet_qos1() {
        let subscribes = vec![SubscribeReasonCode::GrantedQos1];

        let packet = SubackPacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_suback_packet_qos2() {
        let subscribes = vec![SubscribeReasonCode::GrantedQos2];

        let packet = SubackPacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_suback_packet() {
        let reason_codes = vec![SubscribeReasonCode::GrantedQos1];
        let packet = SubackPacket::new(123, reason_codes);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: SUBACK, remaining_length: 4}, packet_identifier: 123, properties: {reason_string: None, user_properties: []}, payload: {reason_codes: [1]}}"
        );
    }
}
