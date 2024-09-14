//! Publish Properties

use std::io::{self, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    common::{
        encodable::{VarBytes, VarInt},
        Decodable, Encodable,
    },
    v5::property::{PropertyType, PropertyTypeError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishProperties {
    total_length: VarInt,
    payload_format_indicator: Option<u8>,
    message_expiry_interval: Option<u32>,
    topic_alias: Option<u16>,
    response_topic: Option<String>,
    correlation_data: Option<VarBytes>,
    user_properties: Vec<(String, String)>,
    subscription_identifiers: Vec<usize>,
    content_type: Option<String>,
}

impl PublishProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn set_payload_format_indicator(&mut self, payload_format_indicator: Option<u8>) {
        self.payload_format_indicator = payload_format_indicator;
        self.fix_total_length();
    }

    pub fn set_message_expiry_interval(&mut self, message_expiry_interval: Option<u32>) {
        self.message_expiry_interval = message_expiry_interval;
        self.fix_total_length();
    }

    pub fn set_topic_alias(&mut self, topic_alias: Option<u16>) {
        self.topic_alias = topic_alias;
        self.fix_total_length();
    }

    pub fn set_response_topic(&mut self, response_topic: Option<String>) {
        self.response_topic = response_topic;
        self.fix_total_length();
    }

    pub fn set_correlation_data(&mut self, correlation_data: Option<Vec<u8>>) {
        self.correlation_data = correlation_data.map(VarBytes);
        self.fix_total_length();
    }

    pub fn add_user_property<S: Into<String>>(&mut self, key: S, value: S) {
        self.user_properties.push((key.into(), value.into()));
        self.fix_total_length();
    }

    pub fn add_subscription_identifier(&mut self, subscription_identifier: usize) {
        self.subscription_identifiers.push(subscription_identifier);
        self.fix_total_length();
    }

    pub fn set_content_type(&mut self, content_type: Option<String>) {
        self.content_type = content_type;
        self.fix_total_length();
    }

    pub fn payload_format_indicator(&self) -> Option<u8> {
        self.payload_format_indicator
    }

    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.message_expiry_interval
    }

    pub fn topic_alias(&self) -> Option<u16> {
        self.topic_alias
    }

    pub fn response_topic(&self) -> &Option<String> {
        &self.response_topic
    }

    pub fn correlation_data(&self) -> &Option<VarBytes> {
        &self.correlation_data
    }

    pub fn user_properties(&self) -> &Vec<(String, String)> {
        &self.user_properties
    }

    pub fn subscription_identifiers(&self) -> &Vec<usize> {
        &self.subscription_identifiers
    }

    pub fn content_type(&self) -> &Option<String> {
        &self.content_type
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        if self.payload_format_indicator.is_some() {
            len += 1 + 1;
        }
        if self.message_expiry_interval.is_some() {
            len += 1 + 4;
        }
        if self.topic_alias.is_some() {
            len += 1 + 2;
        }
        if let Some(response_topic) = &self.response_topic {
            len += 1 + response_topic.encoded_length();
        }
        if let Some(correlation_data) = &self.correlation_data {
            len += 1 + correlation_data.encoded_length();
        }
        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }
        for id in self.subscription_identifiers.iter() {
            len += 1 + VarInt(*id as u32).encoded_length();
        }
        if let Some(content_type) = &self.content_type {
            len += 1 + content_type.encoded_length();
        }

        self.total_length = VarInt(len)
    }
}

impl Encodable for PublishProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

        if let Some(payload_format_indicator) = self.payload_format_indicator {
            writer.write_u8(PropertyType::PayloadFormatIndicator as u8)?;
            writer.write_u8(payload_format_indicator)?;
        }
        if let Some(message_expiry_interval) = self.message_expiry_interval {
            writer.write_u8(PropertyType::MessageExpiryInterval as u8)?;
            writer.write_u32::<BigEndian>(message_expiry_interval)?;
        }
        if let Some(topic_alias) = self.topic_alias {
            writer.write_u8(PropertyType::TopicAlias as u8)?;
            writer.write_u16::<BigEndian>(topic_alias)?;
        }
        if let Some(response_topic) = &self.response_topic {
            writer.write_u8(PropertyType::ResponseTopic as u8)?;
            response_topic.encode(writer)?;
        }
        if let Some(correlation_data) = &self.correlation_data {
            writer.write_u8(PropertyType::CorrelationData as u8)?;
            correlation_data.encode(writer)?;
        }
        for (key, value) in self.user_properties.iter() {
            writer.write_u8(PropertyType::UserProperty as u8)?;
            key.encode(writer)?;
            value.encode(writer)?;
        }
        for id in self.subscription_identifiers.iter() {
            writer.write_u8(PropertyType::SubscriptionIdentifier as u8)?;
            let identifier = VarInt(*id as u32);
            identifier.encode(writer)?;
        }
        if let Some(content_type) = &self.content_type {
            writer.write_u8(PropertyType::ContentType as u8)?;
            content_type.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.total_length.0 + self.total_length.encoded_length()
    }
}

impl Decodable for PublishProperties {
    type Error = PropertyTypeError;
    type Cond = ();

    fn decode_with<R: std::io::Read>(
        reader: &mut R,
        _cond: Self::Cond,
    ) -> Result<Self, Self::Error> {
        let total_length = VarInt::decode(reader)?;

        if total_length.0 == 0 {
            return Ok(Self::default());
        }

        let mut payload_format_indicator = None;
        let mut message_expiry_interval = None;
        let mut topic_alias = None;
        let mut response_topic = None;
        let mut correlation_data = None;
        let mut user_properties = Vec::new();
        let mut subscription_identifiers = Vec::new();
        let mut content_type = None;

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < total_length.0 {
            let prop = reader.read_u8()?;
            cursor += 1;

            match prop.try_into()? {
                PropertyType::PayloadFormatIndicator => {
                    payload_format_indicator = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::MessageExpiryInterval => {
                    message_expiry_interval = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::TopicAlias => {
                    topic_alias = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::ResponseTopic => {
                    let topic = String::decode(reader)?;
                    cursor += 2 + topic.len() as u32;
                    response_topic = Some(topic);
                }
                PropertyType::CorrelationData => {
                    let data = VarBytes::decode(reader)?;
                    cursor += 2 + data.0.len() as u32;
                    correlation_data = Some(data);
                }
                PropertyType::UserProperty => {
                    let key = String::decode(reader)?;
                    let value = String::decode(reader)?;
                    cursor += 2 + key.len() as u32 + 2 + value.len() as u32;
                    user_properties.push((key, value));
                }
                PropertyType::SubscriptionIdentifier => {
                    let id = VarInt::decode(reader)?;
                    cursor += 1 + id.encoded_length();
                    subscription_identifiers.push(id.0 as usize);
                }
                PropertyType::ContentType => {
                    let typ = String::decode(reader)?;
                    cursor += 2 + typ.len() as u32;
                    content_type = Some(typ);
                }
                _ => return Err(PropertyTypeError::InvalidPropertyType(prop)),
            }
        }

        Ok(PublishProperties {
            total_length,
            payload_format_indicator,
            message_expiry_interval,
            topic_alias,
            response_topic,
            correlation_data,
            user_properties,
            subscription_identifiers,
            content_type,
        })
    }
}