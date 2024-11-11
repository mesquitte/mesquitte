//! SUBSCRIBE

use std::{
    fmt::Display,
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
    v5::{
        control::{ControlType, FixedHeader, PacketType, SubscribeProperties, VariableHeaderError},
        packet::PacketError,
    },
};

/// `SUBSCRIBE` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SubscribePacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    properties: SubscribeProperties,
    payload: SubscribePacketPayload,
}

encodable_packet!(SubscribePacket(packet_identifier, properties, payload));

impl SubscribePacket {
    pub fn new(pkid: u16, subscribes: Vec<(TopicFilter, SubscribeOptions)>) -> Self {
        let mut pkt = Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Subscribe), 0),
            packet_identifier: PacketIdentifier(pkid),
            properties: SubscribeProperties::default(),
            payload: SubscribePacketPayload::new(subscribes),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn subscribes(&self) -> &[(TopicFilter, SubscribeOptions)] {
        &self.payload.subscribes[..]
    }

    pub fn properties(&self) -> &SubscribeProperties {
        &self.properties
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn set_properties(&mut self, properties: SubscribeProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }
}

impl DecodablePacket for SubscribePacket {
    type DecodePacketError = SubscribePacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier: PacketIdentifier = PacketIdentifier::decode(reader)?;
        let properties: SubscribeProperties =
            SubscribeProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
        let payload: SubscribePacketPayload = SubscribePacketPayload::decode_with(
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

impl Display for SubscribePacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}, properties: {}, payload: {}}}",
            self.fixed_header, self.packet_identifier, self.properties, self.payload
        )
    }
}

/// Payload of subscribe packet
#[derive(Debug, Eq, PartialEq, Clone)]
struct SubscribePacketPayload {
    subscribes: Vec<(TopicFilter, SubscribeOptions)>,
}

impl SubscribePacketPayload {
    pub fn new(subs: Vec<(TopicFilter, SubscribeOptions)>) -> Self {
        Self { subscribes: subs }
    }
}

impl Encodable for SubscribePacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for (filter, option) in self.subscribes.iter() {
            filter.encode(writer)?;
            option.encode(writer)?;
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

    fn decode_with<R: Read>(reader: &mut R, mut payload_len: u32) -> Result<Self, Self::Error> {
        let mut subs = Vec::new();

        while payload_len > 0 {
            let filter = TopicFilter::decode(reader)?;
            let option = SubscribeOptions::decode(reader)?;

            payload_len -= filter.encoded_length() + option.encoded_length();
            subs.push((filter, option));
        }

        Ok(Self::new(subs))
    }
}

impl Display for SubscribePacketPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{subscribes: [")?;
        let mut iter = self.subscribes.iter();
        if let Some(first) = iter.next() {
            write!(f, "({}, {})", first.0, first.1)?;
            for subscribe in iter {
                write!(f, ", ({}, {})", subscribe.0, subscribe.1)?;
            }
        }
        write!(f, "]}}")
    }
}

/// SubscribePayload options of subscribe packet
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub struct SubscribeOptions {
    qos: QualityOfService,
    no_local: bool,
    retain_as_published: bool,
    retain_handling: RetainHandling,
}

impl SubscribeOptions {
    pub fn qos(&self) -> QualityOfService {
        self.qos
    }
    pub fn set_qos(&mut self, qos: QualityOfService) {
        self.qos = qos;
    }
    pub fn no_local(&self) -> bool {
        self.no_local
    }
    pub fn set_no_local(&mut self, no_local: bool) {
        self.no_local = no_local;
    }
    pub fn retain_as_published(&self) -> bool {
        self.retain_as_published
    }
    pub fn set_retain_as_published(&mut self, retain_as_published: bool) {
        self.retain_as_published = retain_as_published;
    }
    pub fn retain_handling(&self) -> RetainHandling {
        self.retain_handling
    }
    pub fn set_retain_handling(&mut self, retain_handling: RetainHandling) {
        self.retain_handling = retain_handling;
    }
}

impl From<SubscribeOptions> for u8 {
    fn from(value: SubscribeOptions) -> Self {
        let mut byte = value.qos as u8;
        if value.no_local {
            byte |= 0b100;
        }
        if value.retain_as_published {
            byte |= 0b1000;
        }
        byte |= (value.retain_handling as u8) << 4;
        byte
    }
}

impl From<&SubscribeOptions> for u8 {
    fn from(value: &SubscribeOptions) -> Self {
        (*value).into()
    }
}

impl Default for SubscribeOptions {
    fn default() -> Self {
        Self {
            qos: QualityOfService::Level0,
            no_local: Default::default(),
            retain_as_published: Default::default(),
            retain_handling: RetainHandling::SendAtSubscribe,
        }
    }
}

impl Encodable for SubscribeOptions {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(self.into())?;
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for SubscribeOptions {
    type Error = SubscribePacketError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _cond: Self::Cond) -> Result<Self, Self::Error> {
        let options = reader.read_u8()?;

        let requested_qos = options & 0b0000_0011;
        let no_local = (options >> 2 & 0b0000_0001) != 0;
        let retain_as_published = (options >> 3 & 0b0000_0001) != 0;
        let retain_handling = (options >> 4) & 0b0000_0011;

        let qos = match requested_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => return Err(SubscribePacketError::InvalidQualityOfService),
        };

        Ok(Self {
            qos,
            no_local,
            retain_as_published,
            retain_handling: retain_handling.try_into()?,
        })
    }
}

impl Display for SubscribeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{qos: {}, no_local: {}, retain_as_published: {}, retain_handling: {}}}",
            self.qos, self.no_local, self.retain_as_published, self.retain_handling
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum RetainHandling {
    SendAtSubscribe,
    SendAtSubscribeIfNotExist,
    DoNotSend,
}

impl From<RetainHandling> for u8 {
    fn from(value: RetainHandling) -> Self {
        match value {
            RetainHandling::SendAtSubscribe => 0,
            RetainHandling::SendAtSubscribeIfNotExist => 1,
            RetainHandling::DoNotSend => 2,
        }
    }
}

impl From<&RetainHandling> for u8 {
    fn from(value: &RetainHandling) -> Self {
        (*value).into()
    }
}

impl TryFrom<u8> for RetainHandling {
    type Error = SubscribePacketError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RetainHandling::SendAtSubscribe),
            1 => Ok(RetainHandling::SendAtSubscribeIfNotExist),
            2 => Ok(RetainHandling::DoNotSend),
            _ => Err(SubscribePacketError::InvalidRetainHandling),
        }
    }
}

impl Display for RetainHandling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: u8 = self.into();
        write!(f, "{}", value)
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
    #[error("invalid retain handling")]
    InvalidRetainHandling,
}

impl From<TopicFilterDecodeError> for SubscribePacketError {
    fn from(err: TopicFilterDecodeError) -> Self {
        match err {
            TopicFilterDecodeError::IoError(err) => err.into(),
            TopicFilterDecodeError::InvalidTopicFilter(err) => err.into(),
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
                SubscribeOptions::default(),
            ),
            (
                TopicFilter::new("a/c".to_string()).unwrap(),
                SubscribeOptions::default(),
            ),
        ];
        let packet = SubscribePacket::new(32642, subscribes);

        let expected = b"\x82\x0f\x7f\x82\x00\x00\x03\x61\x2f\x62\x00\x00\x03\x61\x2f\x63\x00";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_subscribe_packet_decode_hex() {
        let encoded_data = b"\x82\x0f\xf7\x1a\x00\x00\x03\x62\x2f\x63\x00\x00\x03\x62\x2f\x64\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = SubscribePacket::decode(&mut buf).unwrap();

        let subscribes = vec![
            (
                TopicFilter::new("b/c".to_string()).unwrap(),
                SubscribeOptions::default(),
            ),
            (
                TopicFilter::new("b/d".to_string()).unwrap(),
                SubscribeOptions::default(),
            ),
        ];
        let expected = SubscribePacket::new(63258, subscribes);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_subscribe_packet_basic() {
        let subscribes = vec![
            (
                TopicFilter::new("a/b".to_string()).unwrap(),
                SubscribeOptions::default(),
            ),
            (
                TopicFilter::new("a/c".to_string()).unwrap(),
                SubscribeOptions::default(),
            ),
        ];
        let packet = SubscribePacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubscribePacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_subscribe_packet() {
        let subscriptions = vec![
            (
                TopicFilter::new("test/topic/1").unwrap(),
                SubscribeOptions::default(),
            ),
            (
                TopicFilter::new("test/topic/2").unwrap(),
                SubscribeOptions::default(),
            ),
        ];
        let packet = SubscribePacket::new(2345, subscriptions);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: SUBSCRIBE, remaining_length: 33}, packet_identifier: 2345, properties: {identifier: None, user_properties: []}, payload: {subscribes: [(test/topic/1, {qos: 0, no_local: false, retain_as_published: false, retain_handling: 0}), (test/topic/2, {qos: 0, no_local: false, retain_as_published: false, retain_handling: 0})]}}"
        );
    }
}
