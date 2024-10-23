//! SUBSCRIBE

use std::{
    io::{self, Read, Write},
    string::FromUtf8Error,
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{
        packet::DecodablePacket,
        topic_filter::{TopicFilterDecodeError, TopicFilterError},
        Decodable, Encodable, PacketIdentifier, QualityOfService, TopicFilter,
    },
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `SUBSCRIBE` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SubscribePacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    payload: SubscribePacketPayload,
}

encodable_packet!(SubscribePacket(packet_identifier, payload));

impl SubscribePacket {
    pub fn new(pkid: u16, subscribes: Vec<(TopicFilter, QualityOfService)>) -> SubscribePacket {
        let mut pkt = SubscribePacket {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Subscribe), 0),
            packet_identifier: PacketIdentifier(pkid),
            payload: SubscribePacketPayload::new(subscribes),
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

    pub fn subscribes(&self) -> &[(TopicFilter, QualityOfService)] {
        &self.payload.subscribes[..]
    }
}

impl DecodablePacket for SubscribePacket {
    type DecodePacketError = SubscribePacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        let payload: SubscribePacketPayload = SubscribePacketPayload::decode_with(
            reader,
            fixed_header.remaining_length - packet_identifier.encoded_length(),
        )
        .map_err(PacketError::PayloadError)?;
        Ok(SubscribePacket {
            fixed_header,
            packet_identifier,
            payload,
        })
    }
}

/// Payload of subscribe packet
#[derive(Debug, Eq, PartialEq, Clone)]
struct SubscribePacketPayload {
    subscribes: Vec<(TopicFilter, QualityOfService)>,
}

impl SubscribePacketPayload {
    pub fn new(subs: Vec<(TopicFilter, QualityOfService)>) -> SubscribePacketPayload {
        SubscribePacketPayload { subscribes: subs }
    }
}

impl Encodable for SubscribePacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for (filter, qos) in self.subscribes.iter() {
            filter.encode(writer)?;
            writer.write_u8(*qos as u8)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.subscribes
            .iter()
            .fold(0, |b, a| b + a.0.encoded_length() + 1)
    }
}

impl Decodable for SubscribePacketPayload {
    type Error = SubscribePacketError;
    type Cond = u32;

    fn decode_with<R: Read>(
        reader: &mut R,
        mut payload_len: u32,
    ) -> Result<SubscribePacketPayload, SubscribePacketError> {
        let mut subs = Vec::new();

        while payload_len > 0 {
            let filter = TopicFilter::decode(reader)?;
            let qos = match reader.read_u8()? {
                0 => QualityOfService::Level0,
                1 => QualityOfService::Level1,
                2 => QualityOfService::Level2,
                _ => return Err(SubscribePacketError::InvalidQualityOfService),
            };

            payload_len -= filter.encoded_length() + 1;
            subs.push((filter, qos));
        }

        Ok(SubscribePacketPayload::new(subs))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubscribePacketError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error("invalid quality of service")]
    InvalidQualityOfService,
    #[error(transparent)]
    TopicFilterError(#[from] TopicFilterError),
}

impl From<TopicFilterDecodeError> for SubscribePacketError {
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
    fn test_subscribe_packet_encode_hex() {
        let subscribes = vec![
            (
                TopicFilter::new("a/b".to_string()).unwrap(),
                QualityOfService::Level0,
            ),
            (
                TopicFilter::new("a/c".to_string()).unwrap(),
                QualityOfService::Level0,
            ),
        ];
        let packet = SubscribePacket::new(40300, subscribes);

        let expected = b"\x82\x0e\x9d\x6c\x00\x03\x61\x2f\x62\x00\x00\x03\x61\x2f\x63\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    pub fn test_subscribe_packet_decode_hex() {
        let encoded_data = b"\x82\x0e\x9d\x6f\x00\x03\x61\x2f\x62\x01\x00\x03\x61\x2f\x63\x01";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = SubscribePacket::decode(&mut buf).unwrap();

        let subscribes = vec![
            (
                TopicFilter::new("a/b".to_string()).unwrap(),
                QualityOfService::Level1,
            ),
            (
                TopicFilter::new("a/c".to_string()).unwrap(),
                QualityOfService::Level1,
            ),
        ];
        let expected = SubscribePacket::new(40303, subscribes);

        assert_eq!(expected, packet);
    }

    #[test]
    pub fn test_subscribe_packet_basic() {
        let subscribes = vec![
            (
                TopicFilter::new("a/b".to_string()).unwrap(),
                QualityOfService::Level0,
            ),
            (
                TopicFilter::new("a/c".to_string()).unwrap(),
                QualityOfService::Level1,
            ),
        ];
        let packet = SubscribePacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubscribePacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }
}
