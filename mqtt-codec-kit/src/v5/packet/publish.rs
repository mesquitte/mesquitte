//! PUBLISH

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use crate::{
    common::{
        packet::{DecodablePacket, EncodablePacket},
        qos::QoSWithPacketIdentifier,
        Decodable, Encodable, PacketIdentifier, TopicName, TopicNameRef,
    },
    v5::{
        control::{FixedHeader, PacketType, PublishProperties, VariableHeaderError},
        packet::PacketError,
    },
};

/// `PUBLISH` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PublishPacket {
    fixed_header: FixedHeader,
    topic_name: TopicName,
    packet_identifier: Option<PacketIdentifier>,
    properties: PublishProperties,
    payload: Vec<u8>,
}

encodable_packet!(PublishPacket(
    topic_name,
    packet_identifier,
    properties,
    payload
));

impl PublishPacket {
    pub fn new<P: Into<Vec<u8>>>(
        topic_name: TopicName,
        qos: QoSWithPacketIdentifier,
        payload: P,
    ) -> Self {
        let (qos, pkid) = qos.split();
        let mut pkt = Self {
            fixed_header: FixedHeader::new(PacketType::publish(qos), 0),
            topic_name,
            packet_identifier: pkid.map(PacketIdentifier),
            properties: PublishProperties::default(),
            payload: payload.into(),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn set_dup(&mut self, dup: bool) {
        self.fixed_header
            .packet_type
            .update_flags(|flags| (flags & !(1 << 3)) | (dup as u8) << 3)
    }

    pub fn dup(&self) -> bool {
        self.fixed_header.packet_type.flags() & 0x80 != 0
    }

    pub fn set_qos(&mut self, qos: QoSWithPacketIdentifier) {
        let (qos, pkid) = qos.split();
        self.fixed_header
            .packet_type
            .update_flags(|flags| (flags & !0b0110) | (qos as u8) << 1);
        self.packet_identifier = pkid.map(PacketIdentifier);
        self.fix_header_remaining_len();
    }

    pub fn qos(&self) -> QoSWithPacketIdentifier {
        match self.packet_identifier {
            None => QoSWithPacketIdentifier::Level0,
            Some(pkid) => {
                let qos_val = (self.fixed_header.packet_type.flags() & 0b0110) >> 1;
                match qos_val {
                    1 => QoSWithPacketIdentifier::Level1(pkid.0),
                    2 => QoSWithPacketIdentifier::Level2(pkid.0),
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn set_retain(&mut self, ret: bool) {
        self.fixed_header
            .packet_type
            .update_flags(|flags| (flags & !0b0001) | (ret as u8))
    }

    pub fn retain(&self) -> bool {
        self.fixed_header.packet_type.flags() & 0b0001 != 0
    }

    pub fn set_topic_name(&mut self, topic_name: TopicName) {
        self.topic_name = topic_name;
        self.fix_header_remaining_len();
    }

    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn set_payload<P: Into<Vec<u8>>>(&mut self, payload: P) {
        self.payload = payload.into();
        self.fix_header_remaining_len();
    }

    pub fn properties(&self) -> &PublishProperties {
        &self.properties
    }

    pub fn set_properties(&mut self, properties: PublishProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }
}

impl DecodablePacket for PublishPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let topic_name = TopicName::decode(reader)?;

        let qos = (fixed_header.packet_type.flags() & 0b0110) >> 1;
        let packet_identifier = if qos > 0 {
            Some(PacketIdentifier::decode(reader)?)
        } else {
            None
        };

        let properties: PublishProperties =
            PublishProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;

        let vhead_len = topic_name.encoded_length()
            + packet_identifier
                .as_ref()
                .map(|x| x.encoded_length())
                .unwrap_or(0)
            + properties.encoded_length();

        let payload_len = fixed_header.remaining_length - vhead_len;

        let payload = Vec::<u8>::decode_with(reader, Some(payload_len))?;

        Ok(Self {
            fixed_header,
            topic_name,
            packet_identifier,
            properties,
            payload,
        })
    }
}

impl Display for PublishPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, topic_name: {}",
            self.fixed_header, self.topic_name
        )?;
        match self.packet_identifier {
            Some(packet_identifier) => write!(f, ", packet_identifier: {}", packet_identifier)?,
            None => write!(f, ", packet_identifier: None")?,
        };

        write!(f, ", properties: {}", self.properties)?;

        match std::str::from_utf8(&self.payload) {
            Ok(s) if s.chars().all(|c| c.is_ascii_graphic() || c == ' ') => {
                write!(f, ", payload: {}", s)?;
            }
            _ => {
                write!(f, ", payload: [")?;
                let mut iter = self.payload.iter();
                if let Some(first) = iter.next() {
                    write!(f, "{}", first)?;
                    for byte in iter {
                        write!(f, ", {}", byte)?;
                    }
                }
                write!(f, "]")?;
            }
        };
        write!(f, "}}")
    }
}

/// `PUBLISH` packet by reference, for encoding only
pub struct PublishPacketRef<'a> {
    fixed_header: FixedHeader,
    topic_name: &'a TopicNameRef,
    packet_identifier: Option<PacketIdentifier>,
    payload: &'a [u8],
}

impl<'a> PublishPacketRef<'a> {
    pub fn new(
        topic_name: &'a TopicNameRef,
        qos: QoSWithPacketIdentifier,
        payload: &'a [u8],
    ) -> Self {
        let (qos, pkid) = qos.split();

        let mut pk = Self {
            fixed_header: FixedHeader::new(PacketType::publish(qos), 0),
            topic_name,
            packet_identifier: pkid.map(PacketIdentifier),
            payload,
        };
        pk.fix_header_remaining_len();
        pk
    }

    fn fix_header_remaining_len(&mut self) {
        self.fixed_header.remaining_length = self.topic_name.encoded_length()
            + self.packet_identifier.encoded_length()
            + self.payload.encoded_length();
    }
}

impl EncodablePacket for PublishPacketRef<'_> {
    type Output = FixedHeader;

    fn fixed_header(&self) -> &Self::Output {
        &self.fixed_header
    }

    fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.topic_name.encode(writer)?;
        self.packet_identifier.encode(writer)?;
        self.payload.encode(writer)
    }

    fn encoded_packet_length(&self) -> u32 {
        self.topic_name.encoded_length()
            + self.packet_identifier.encoded_length()
            + self.payload.encoded_length()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    use crate::common::topic_name::TopicName;
    use crate::common::{Decodable, Encodable};

    #[test]
    fn test_publish_packet_encode_hex() {
        let mut packet = PublishPacket::new(
            TopicName::new("a/b").unwrap(),
            QoSWithPacketIdentifier::Level1(26373),
            b"{\"msg\":\"hello, world!\"}",
        );

        let mut properties = PublishProperties::default();
        properties.set_payload_format_indicator(Some(1));
        properties.add_user_property("a", "b");

        packet.set_retain(true);
        packet.set_properties(properties);

        let expected = b"\x33\x28\x00\x03\x61\x2f\x62\x67\x05\x09\x01\x01\x26\x00\x01\x61\x00\x01\x62\x7b\x22\x6d\x73\x67\x22\x3a\x22\x68\x65\x6c\x6c\x6f\x2c\x20\x77\x6f\x72\x6c\x64\x21\x22\x7d";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_publish_packet_decode_hex() {
        let encoded_data = b"\x30\x1f\x00\x03\x61\x2f\x62\x02\x01\x00\x7b\x22\x6d\x73\x67\x22\x3a\x22\x68\x65\x6c\x6c\x6f\x2c\x20\x77\x6f\x72\x6c\x64\x21\x22\x7d";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = PublishPacket::decode(&mut buf).unwrap();

        let mut expected = PublishPacket::new(
            TopicName::new("a/b").unwrap(),
            QoSWithPacketIdentifier::Level0,
            b"{\"msg\":\"hello, world!\"}",
        );

        let mut properties = PublishProperties::default();
        properties.set_payload_format_indicator(Some(0));

        expected.set_properties(properties);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_publish_packet_basic() {
        let packet = PublishPacket::new(
            TopicName::new("a/b".to_owned()).unwrap(),
            QoSWithPacketIdentifier::Level2(10),
            b"Hello world!".to_vec(),
        );

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = PublishPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_readable_publish_packet() {
        let packet = PublishPacket::new(
            TopicName::new("a/b".to_owned()).unwrap(),
            QoSWithPacketIdentifier::Level2(10),
            b"Hello world!".to_vec(),
        );

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBLISH, remaining_length: 20}, topic_name: a/b, packet_identifier: 10, properties: {payload_format_indicator: None, message_expiry_interval: None, topic_alias: None, response_topic: None, correlation_data: None, user_properties: [], subscription_identifier: None, content_type: None}, payload: Hello world!}"
        );
    }

    #[test]
    fn test_display_non_readable_publish_packet() {
        let packet = PublishPacket::new(
            TopicName::new("a/b".to_owned()).unwrap(),
            QoSWithPacketIdentifier::Level2(10),
            vec![1, 2, 3, 4],
        );

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: PUBLISH, remaining_length: 12}, topic_name: a/b, packet_identifier: 10, properties: {payload_format_indicator: None, message_expiry_interval: None, topic_alias: None, response_topic: None, correlation_data: None, user_properties: [], subscription_identifier: None, content_type: None}, payload: [1, 2, 3, 4]}"
        );
    }
}
