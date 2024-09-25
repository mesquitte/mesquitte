//! SUBACK

use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{packet::DecodablePacket, Decodable, Encodable, PacketIdentifier},
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
    pub fn new(pkid: u16, reason_codes: Vec<SubscribeReasonCode>) -> SubackPacket {
        let mut pkt = SubackPacket {
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

        Ok(SubackPacket {
            fixed_header,
            packet_identifier,
            properties,
            payload,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct SubackPacketPayload {
    reason_codes: Vec<SubscribeReasonCode>,
}

impl SubackPacketPayload {
    pub fn new(reason_codes: Vec<SubscribeReasonCode>) -> SubackPacketPayload {
        SubackPacketPayload { reason_codes }
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

    fn decode_with<R: Read>(
        reader: &mut R,
        payload_len: u32,
    ) -> Result<SubackPacketPayload, SubackPacketError> {
        let mut subs = Vec::new();

        for _ in 0..payload_len {
            subs.push(reader.read_u8()?.try_into()?);
        }

        Ok(SubackPacketPayload::new(subs))
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

impl SubscribeReasonCode {
    /// Get the value
    fn to_u8(self) -> u8 {
        match self {
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

/// Create `SubscribeReasonCode` from value
impl TryFrom<u8> for SubscribeReasonCode {
    type Error = SubackPacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(Self::GrantedQos0),
            NO_MATCHING_SUBSCRIBERS => Ok(Self::GrantedQos1),
            GRANTED_QOS_2 => Ok(Self::GrantedQos2),
            UNSPECIFIED_ERROR => Ok(Self::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(Self::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(Self::NotAuthorized),
            TOPIC_FILTER_INVALID => Ok(Self::TopicFilterInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(Self::PacketIdentifierInUse),
            QUOTA_EXCEEDED => Ok(Self::QuotaExceeded),
            SHARED_SUBSCRIPTION_NOT_SUPPORTED => Ok(Self::SharedSubscriptionNotSupported),
            SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED => Ok(Self::SubscriptionIdentifiersNotSupported),
            WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED => Ok(Self::WildcardSubscriptionsNotSupported),
            v => Err(SubackPacketError::InvalidSubscribeReasonCode(v)),
        }
    }
}

impl Encodable for SubscribeReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.to_u8())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for SubscribeReasonCode {
    type Error = SubackPacketError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<SubscribeReasonCode, SubackPacketError> {
        reader
            .read_u8()
            .map(SubscribeReasonCode::try_from)?
            .map_err(From::from)
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
    pub fn test_suback_packet_encode_hex() {
        let packet = SubackPacket::new(63254, vec![SubscribeReasonCode::GrantedQos0]);

        let expected = b"\x90\x04\xf7\x16\x00\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_suback_packet_decode_hex() {
        let encoded_data = b"\x90\x04\xf7\x17\x00\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = SubackPacket::decode(&mut buf).unwrap();

        let expected = SubackPacket::new(63255, vec![SubscribeReasonCode::GrantedQos0]);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_suback_packet_basic() {
        let subscribes = vec![SubscribeReasonCode::GrantedQos0];

        let packet = SubackPacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
