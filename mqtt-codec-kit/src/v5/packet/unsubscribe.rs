//! UNSUBSCRIBE

use std::{
    io::{self, Read, Write},
    string::FromUtf8Error,
};

use crate::{
    common::{
        packet::DecodablePacket,
        topic_filter::{TopicFilterDecodeError, TopicFilterError},
        Decodable, Encodable, PacketIdentifier, TopicFilter,
    },
    v5::{
        control::{
            ControlType, FixedHeader, PacketType, UnsubscribeProperties, VariableHeaderError,
        },
        packet::PacketError,
    },
};

/// `UNSUBSCRIBE` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UnsubscribePacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    properties: UnsubscribeProperties,
    payload: UnsubscribePacketPayload,
}

encodable_packet!(UnsubscribePacket(packet_identifier, properties, payload));

impl UnsubscribePacket {
    pub fn new(pkid: u16, subscribes: Vec<TopicFilter>) -> UnsubscribePacket {
        let mut pk = UnsubscribePacket {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Unsubscribe), 0),
            packet_identifier: PacketIdentifier(pkid),
            properties: UnsubscribeProperties::default(),
            payload: UnsubscribePacketPayload::new(subscribes),
        };
        pk.fix_header_remaining_len();
        pk
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn subscribes(&self) -> &[TopicFilter] {
        &self.payload.subscribes[..]
    }
}

impl DecodablePacket for UnsubscribePacket {
    type DecodePacketError = UnsubscribePacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        let properties = UnsubscribeProperties::decode(reader)
            .map_err(VariableHeaderError::PropertyTypeError)?;
        let payload: UnsubscribePacketPayload = UnsubscribePacketPayload::decode_with(
            reader,
            fixed_header.remaining_length
                - packet_identifier.encoded_length()
                - properties.encoded_length(),
        )
        .map_err(PacketError::PayloadError)?;

        Ok(UnsubscribePacket {
            fixed_header,
            packet_identifier,
            properties,
            payload,
        })
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct UnsubscribePacketPayload {
    subscribes: Vec<TopicFilter>,
}

impl UnsubscribePacketPayload {
    pub fn new(subs: Vec<TopicFilter>) -> UnsubscribePacketPayload {
        UnsubscribePacketPayload { subscribes: subs }
    }
}

impl Encodable for UnsubscribePacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for filter in self.subscribes.iter() {
            filter.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.subscribes
            .iter()
            .fold(0, |b, a| b + a.encoded_length())
    }
}

impl Decodable for UnsubscribePacketPayload {
    type Error = UnsubscribePacketError;
    type Cond = u32;

    fn decode_with<R: Read>(
        reader: &mut R,
        mut payload_len: u32,
    ) -> Result<UnsubscribePacketPayload, UnsubscribePacketError> {
        let mut subs = Vec::new();

        while payload_len > 0 {
            let filter = TopicFilter::decode(reader)?;
            payload_len -= filter.encoded_length();
            subs.push(filter);
        }

        Ok(UnsubscribePacketPayload::new(subs))
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum UnsubscribePacketError {
    IoError(#[from] io::Error),
    FromUtf8Error(#[from] FromUtf8Error),
    TopicFilterError(#[from] TopicFilterError),
}

impl From<TopicFilterDecodeError> for UnsubscribePacketError {
    fn from(e: TopicFilterDecodeError) -> Self {
        match e {
            TopicFilterDecodeError::IoError(e) => e.into(),
            TopicFilterDecodeError::InvalidTopicFilter(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    pub fn test_unsubscribe_packet_encode_hex() {
        let packet = UnsubscribePacket::new(63256, vec![TopicFilter::new("a/b").unwrap()]);

        let expected = b"\xa2\x08\xf7\x18\x00\x00\x03\x61\x2f\x62";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_unsubscribe_packet_decode_hex() {
        let encoded_data = b"\xa2\x08\xf7\x19\x00\x00\x03\x61\x2f\x63";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = UnsubscribePacket::decode(&mut buf).unwrap();

        let expected = UnsubscribePacket::new(63257, vec![TopicFilter::new("a/c").unwrap()]);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_unsubscribe_packet_basic() {
        let subscribes = vec![
            TopicFilter::new("a/b".to_string()).unwrap(),
            TopicFilter::new("a/c".to_string()).unwrap(),
        ];
        let packet = UnsubscribePacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = UnsubscribePacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
