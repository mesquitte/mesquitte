//! UNSUBACK

use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{packet::DecodablePacket, Decodable, Encodable, PacketIdentifier},
    v5::{
        control::{ControlType, FixedHeader, PacketType, UnsubackProperties, VariableHeaderError},
        packet::PacketError,
        reason_code_value::*,
    },
};

/// `UNSUBACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UnsubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    properties: UnsubackProperties,
    payload: UnsubackPacketPayload,
}

encodable_packet!(UnsubackPacket(packet_identifier, properties, payload));

impl UnsubackPacket {
    pub fn new(pkid: u16, reason_codes: Vec<UnsubscribeReasonCode>) -> Self {
        let mut pkt = Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::UnsubscribeAcknowledgement),
                0,
            ),
            packet_identifier: PacketIdentifier(pkid),
            properties: UnsubackProperties::default(),
            payload: UnsubackPacketPayload::new(reason_codes),
        };

        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn properties(&self) -> &UnsubackProperties {
        &self.properties
    }

    pub fn reason_code(&self) -> &[UnsubscribeReasonCode] {
        &self.payload.reason_codes[..]
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: UnsubackProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }
}

impl DecodablePacket for UnsubackPacket {
    type DecodePacketError = UnsubackPacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let mut remaining_len = fixed_header.remaining_length;

        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        let properties =
            UnsubackProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
        remaining_len = remaining_len
            .checked_sub(packet_identifier.encoded_length() + properties.encoded_length())
            .ok_or(VariableHeaderError::InvalidRemainingLength)?;

        let payload = if remaining_len > 0 {
            let payload: UnsubackPacketPayload =
                UnsubackPacketPayload::decode_with(reader, remaining_len)
                    .map_err(PacketError::PayloadError)?;
            payload
        } else {
            UnsubackPacketPayload::default()
        };

        Ok(Self {
            fixed_header,
            packet_identifier,
            properties,
            payload,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default)]
struct UnsubackPacketPayload {
    reason_codes: Vec<UnsubscribeReasonCode>,
}

impl UnsubackPacketPayload {
    pub fn new(reason_codes: Vec<UnsubscribeReasonCode>) -> Self {
        Self { reason_codes }
    }
}

impl Encodable for UnsubackPacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for code in self.reason_codes.iter() {
            writer.write_u8(*code as u8)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.reason_codes.len() as u32
    }
}

impl Decodable for UnsubackPacketPayload {
    type Error = UnsubackPacketError;
    type Cond = u32;

    fn decode_with<R: Read>(reader: &mut R, payload_len: u32) -> Result<Self, Self::Error> {
        let mut unsubscribes = Vec::new();

        for _ in 0..payload_len {
            unsubscribes.push(reader.read_u8()?.try_into()?);
        }

        Ok(Self::new(unsubscribes))
    }
}

/// Reason code for `UNSUBACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum UnsubscribeReasonCode {
    Success,
    NoSubscriptionExisted,
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicFilterInvalid,
    PacketIdentifierInUse,
}

impl From<UnsubscribeReasonCode> for u8 {
    fn from(value: UnsubscribeReasonCode) -> Self {
        match value {
            UnsubscribeReasonCode::Success => SUCCESS,
            UnsubscribeReasonCode::NoSubscriptionExisted => NO_SUBSCRIPTION_EXISTED,
            UnsubscribeReasonCode::UnspecifiedError => UNSPECIFIED_ERROR,
            UnsubscribeReasonCode::ImplementationSpecificError => IMPLEMENTATION_SPECIFIC_ERROR,
            UnsubscribeReasonCode::NotAuthorized => NOT_AUTHORIZED,
            UnsubscribeReasonCode::TopicFilterInvalid => TOPIC_FILTER_INVALID,
            UnsubscribeReasonCode::PacketIdentifierInUse => PACKET_IDENTIFIER_IN_USE,
        }
    }
}

impl From<&UnsubscribeReasonCode> for u8 {
    fn from(value: &UnsubscribeReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `UnsubackReasonCode` from value
impl TryFrom<u8> for UnsubscribeReasonCode {
    type Error = UnsubackPacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(UnsubscribeReasonCode::Success),
            NO_SUBSCRIPTION_EXISTED => Ok(UnsubscribeReasonCode::NoSubscriptionExisted),
            UNSPECIFIED_ERROR => Ok(UnsubscribeReasonCode::UnspecifiedError),
            IMPLEMENTATION_SPECIFIC_ERROR => Ok(UnsubscribeReasonCode::ImplementationSpecificError),
            NOT_AUTHORIZED => Ok(UnsubscribeReasonCode::NotAuthorized),
            TOPIC_FILTER_INVALID => Ok(UnsubscribeReasonCode::TopicFilterInvalid),
            PACKET_IDENTIFIER_IN_USE => Ok(UnsubscribeReasonCode::PacketIdentifierInUse),
            v => Err(UnsubackPacketError::InvalidUnsubscribeReasonCode(v)),
        }
    }
}

impl Encodable for UnsubscribeReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for UnsubscribeReasonCode {
    type Error = UnsubackPacketError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<UnsubscribeReasonCode, UnsubackPacketError> {
        reader
            .read_u8()
            .map(UnsubscribeReasonCode::try_from)?
            .map_err(From::from)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnsubackPacketError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid unsubscribe reason code ({0})")]
    InvalidUnsubscribeReasonCode(u8),
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    pub fn test_unsuback_packet_encode_hex() {
        let packet = UnsubackPacket::new(63256, vec![UnsubscribeReasonCode::Success]);

        let expected = b"\xb0\x04\xf7\x18\x00\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_unsuback_packet_decode_hex() {
        let encoded_data = b"\xb0\x04\xf7\x19\x00\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = UnsubackPacket::decode(&mut buf).unwrap();

        let expected = UnsubackPacket::new(63257, vec![UnsubscribeReasonCode::Success]);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_unsuback_packet_basic() {
        let unsubscribes = vec![UnsubscribeReasonCode::Success];

        let packet = UnsubackPacket::new(10001, unsubscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = UnsubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_unsuback_packet_with_properties() {
        let unsubscribes = vec![UnsubscribeReasonCode::Success];

        let mut properties = UnsubackProperties::default();
        properties.set_reason_string(Some("Ok".to_string()));

        let mut packet = UnsubackPacket::new(10001, unsubscribes);
        packet.set_properties(properties);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = UnsubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
