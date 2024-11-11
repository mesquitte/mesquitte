//! UNSUBSCRIBE

use std::{
    fmt::Display,
    io::{self, Read, Write},
    string::FromUtf8Error,
};

use crate::{
    common::{
        packet::DecodablePacket,
        topic_filter::{TopicFilterDecodeError, TopicFilterError},
        Decodable, Encodable, PacketIdentifier, TopicFilter,
    },
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `UNSUBSCRIBE` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct UnsubscribePacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    payload: UnsubscribePacketPayload,
}

encodable_packet!(UnsubscribePacket(packet_identifier, payload));

impl UnsubscribePacket {
    pub fn new(pkid: u16, topics: Vec<TopicFilter>) -> Self {
        let mut pkt = Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Unsubscribe), 0),
            packet_identifier: PacketIdentifier(pkid),
            payload: UnsubscribePacketPayload::new(topics),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn topic_filters(&self) -> &[TopicFilter] {
        &self.payload.topic_filters[..]
    }
}

impl DecodablePacket for UnsubscribePacket {
    type DecodePacketError = UnsubscribePacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        let payload: UnsubscribePacketPayload = UnsubscribePacketPayload::decode_with(
            reader,
            fixed_header.remaining_length - packet_identifier.encoded_length(),
        )
        .map_err(PacketError::PayloadError)?;
        Ok(Self {
            fixed_header,
            packet_identifier,
            payload,
        })
    }
}

impl Display for UnsubscribePacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}, payload: {}}}",
            self.fixed_header, self.packet_identifier, self.payload
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct UnsubscribePacketPayload {
    topic_filters: Vec<TopicFilter>,
}

impl UnsubscribePacketPayload {
    pub fn new(topic_filters: Vec<TopicFilter>) -> Self {
        Self { topic_filters }
    }
}

impl Encodable for UnsubscribePacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for filter in self.topic_filters.iter() {
            filter.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.topic_filters
            .iter()
            .fold(0, |b, a| b + a.encoded_length())
    }
}

impl Decodable for UnsubscribePacketPayload {
    type Error = UnsubscribePacketError;
    type Cond = u32;

    fn decode_with<R: Read>(reader: &mut R, mut payload_len: u32) -> Result<Self, Self::Error> {
        let mut topic_filters = Vec::new();

        while payload_len > 0 {
            let filter = TopicFilter::decode(reader)?;
            payload_len -= filter.encoded_length();
            topic_filters.push(filter);
        }

        Ok(Self::new(topic_filters))
    }
}

impl Display for UnsubscribePacketPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{topic_filters: [")?;
        let mut iter = self.topic_filters.iter();
        if let Some(first) = iter.next() {
            write!(f, "{}", first)?;
            for topic_filter in iter {
                write!(f, ", {}", topic_filter)?;
            }
        }
        write!(f, "]}}")
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
    fn test_unsubscribe_packet_encode_hex() {
        let packet = UnsubscribePacket::new(40304, vec![TopicFilter::new("a/b").unwrap()]);

        let expected = b"\xa2\x07\x9d\x70\x00\x03\x61\x2f\x62";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_unsubscribe_packet_decode_hex() {
        let encoded_data = b"\xa2\x07\x9d\x71\x00\x03\x61\x2f\x63";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = UnsubscribePacket::decode(&mut buf).unwrap();

        let expected = UnsubscribePacket::new(40305, vec![TopicFilter::new("a/c").unwrap()]);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_unsubscribe_packet_basic() {
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

    #[test]
    fn test_display_unsubscribe_packet() {
        let topics = vec![
            TopicFilter::new("test/topic/1").unwrap(),
            TopicFilter::new("test/topic/2").unwrap(),
        ];
        let packet = UnsubscribePacket::new(2345, topics);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: UNSUBSCRIBE, remaining_length: 30}, packet_identifier: 2345, payload: {topic_filters: [test/topic/1, test/topic/2]}}"
        );
    }
}
