//! CONNECT

use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    common::{
        encodable::{VarBytes, VarInt},
        packet::DecodablePacket,
        protocol_level::SPEC_5_0,
        ConnectFlags, ConnectFlagsError, Decodable, Encodable, KeepAlive, ProtocolLevel,
        ProtocolName, QualityOfService, TopicName, TopicNameDecodeError, TopicNameError,
    },
    v5::{
        control::{ControlType, FixedHeader, PacketType, VariableHeaderError},
        packet::PacketError,
        property::{PropertyType, PropertyTypeError},
    },
};

/// `CONNECT` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ConnectPacket {
    fixed_header: FixedHeader,
    protocol_name: ProtocolName,
    protocol_level: ProtocolLevel,
    flags: ConnectFlags,
    keep_alive: KeepAlive,
    properties: ConnectProperties,
    payload: ConnectPayload,
}

encodable_packet!(ConnectPacket(
    protocol_name,
    protocol_level,
    flags,
    keep_alive,
    properties,
    payload
));

impl ConnectPacket {
    pub fn new<C>(client_identifier: C) -> ConnectPacket
    where
        C: Into<String>,
    {
        ConnectPacket::with_level("MQTT", client_identifier, SPEC_5_0)
            .expect("SPEC_5_0 should always be valid")
    }

    pub fn with_level<P, C>(
        protoname: P,
        client_identifier: C,
        level: u8,
    ) -> Result<ConnectPacket, VariableHeaderError>
    where
        P: Into<String>,
        C: Into<String>,
    {
        let protocol_level = ProtocolLevel::try_from(level)?;
        let mut pkt = ConnectPacket {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Connect), 0),
            protocol_name: ProtocolName(protoname.into()),
            protocol_level,
            flags: ConnectFlags::empty(),
            keep_alive: KeepAlive(0),
            properties: ConnectProperties::default(),
            payload: ConnectPayload::new(client_identifier.into()),
        };

        pkt.fix_header_remaining_len();

        Ok(pkt)
    }

    pub fn set_keep_alive(&mut self, keep_alive: u16) {
        self.keep_alive = KeepAlive(keep_alive);
    }

    pub fn set_username(&mut self, name: Option<String>) {
        self.flags.username = name.is_some();
        self.payload.username = name;
        self.fix_header_remaining_len();
    }

    pub fn set_will(&mut self, will_message: Option<LastWill>) {
        self.flags.will_flag = will_message.is_some();

        self.payload.last_will = will_message;

        self.fix_header_remaining_len();
    }

    pub fn set_password(&mut self, password: Option<String>) {
        self.flags.password = password.is_some();
        self.payload.password = password;
        self.fix_header_remaining_len();
    }

    pub fn set_client_identifier<I: Into<String>>(&mut self, id: I) {
        self.payload.client_identifier = id.into();
        self.fix_header_remaining_len();
    }

    pub fn set_will_retain(&mut self, will_retain: bool) {
        self.flags.will_retain = will_retain;

        if let Some(will) = &self.payload.last_will {
            let mut last_will = will.clone();
            last_will.retain = will_retain;
            self.payload.last_will = Some(last_will);
        }
    }

    pub fn set_will_qos(&mut self, will_qos: u8) {
        let qos = match will_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => QualityOfService::Level0,
        };

        self.flags.will_qos = qos as u8;

        if let Some(will) = &self.payload.last_will {
            let mut last_will = will.clone();

            last_will.qos = qos;
            self.payload.last_will = Some(last_will);
        }
    }

    pub fn set_clean_session(&mut self, clean_session: bool) {
        self.flags.clean_session = clean_session;
    }

    pub fn set_properties(&mut self, properties: ConnectProperties) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    pub fn username(&self) -> Option<&str> {
        self.payload.username.as_ref().map(|x| &x[..])
    }

    pub fn password(&self) -> Option<&str> {
        self.payload.password.as_ref().map(|x| &x[..])
    }

    pub fn will(&self) -> Option<LastWill> {
        self.payload.last_will.clone()
    }

    pub fn will_retain(&self) -> bool {
        self.flags.will_retain
    }

    pub fn will_qos(&self) -> u8 {
        self.flags.will_qos
    }

    pub fn client_identifier(&self) -> &str {
        &self.payload.client_identifier[..]
    }

    pub fn protocol_name(&self) -> &str {
        &self.protocol_name.0
    }

    pub fn protocol_level(&self) -> ProtocolLevel {
        self.protocol_level
    }

    pub fn clean_session(&self) -> bool {
        self.flags.clean_session
    }

    pub fn keep_alive(&self) -> u16 {
        self.keep_alive.0
    }

    pub fn properties(&self) -> &ConnectProperties {
        &self.properties
    }

    /// Read back the "reserved" Connect flag bit 0. For compliant implementations this should
    /// always be false.
    pub fn reserved_flag(&self) -> bool {
        self.flags.reserved
    }
}

impl DecodablePacket for ConnectPacket {
    type DecodePacketError = ConnectPacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let protoname: ProtocolName = Decodable::decode(reader)?;
        let protocol_level: ProtocolLevel =
            Decodable::decode(reader).map_err(VariableHeaderError::InvalidProtocolLevel)?;
        let flags: ConnectFlags = Decodable::decode(reader).map_err(|e| match e {
            ConnectFlagsError::IoError(err) => VariableHeaderError::IoError(err),
            ConnectFlagsError::InvalidReservedFlag => VariableHeaderError::InvalidReservedFlag,
        })?;
        let keep_alive: KeepAlive = Decodable::decode(reader)?;

        let properties: ConnectProperties =
            Decodable::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;

        let payload: ConnectPayload =
            Decodable::decode_with(reader, Some(flags)).map_err(PacketError::PayloadError)?;

        Ok(ConnectPacket {
            fixed_header,
            protocol_name: protoname,
            protocol_level,
            flags,
            keep_alive,
            properties,
            payload,
        })
    }
}

/// Properties for connect packet
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectProperties {
    total_length: VarInt,
    /// Expiry interval property after loosing connection
    session_expiry_interval: Option<u32>,
    /// Maximum simultaneous packets
    receive_maximum: Option<u16>,
    /// Maximum packet size
    max_packet_size: Option<u32>,
    /// Maximum mapping integer for a topic
    topic_alias_max: Option<u16>,
    /// Response information
    request_response_info: Option<u8>,
    /// Problem information
    request_problem_info: Option<u8>,
    /// List of user properties
    user_properties: Vec<(String, String)>,
    /// Method of authentication
    authentication_method: Option<String>,
    /// Authentication data
    authentication_data: Option<VarBytes>,
}

impl ConnectProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn set_session_expiry_interval(&mut self, session_expiry_interval: Option<u32>) {
        self.session_expiry_interval = session_expiry_interval;
        self.fix_total_length();
    }

    pub fn set_receive_maximum(&mut self, receive_maximum: Option<u16>) {
        self.receive_maximum = receive_maximum;
        self.fix_total_length();
    }

    pub fn set_max_packet_size(&mut self, max_packet_size: Option<u32>) {
        self.max_packet_size = max_packet_size;
        self.fix_total_length();
    }

    pub fn set_topic_alias_max(&mut self, topic_alias_max: Option<u16>) {
        self.topic_alias_max = topic_alias_max;
        self.fix_total_length();
    }

    pub fn set_request_response_info(&mut self, request_response_info: Option<u8>) {
        self.request_response_info = request_response_info;
        self.fix_total_length();
    }

    pub fn set_request_problem_info(&mut self, request_problem_info: Option<u8>) {
        self.request_problem_info = request_problem_info;
        self.fix_total_length();
    }

    pub fn add_user_property<S: Into<String>>(&mut self, key: S, value: S) {
        self.user_properties.push((key.into(), value.into()));
        self.fix_total_length();
    }

    pub fn set_authentication_method(&mut self, authentication_method: Option<String>) {
        self.authentication_method = authentication_method;
        self.fix_total_length();
    }

    pub fn set_authentication_data(&mut self, authentication_data: Option<Vec<u8>>) {
        self.authentication_data = authentication_data.map(VarBytes);
        self.fix_total_length();
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.session_expiry_interval
    }

    pub fn receive_maximum(&self) -> Option<u16> {
        self.receive_maximum
    }

    pub fn max_packet_size(&self) -> Option<u32> {
        self.max_packet_size
    }

    pub fn topic_alias_max(&self) -> Option<u16> {
        self.topic_alias_max
    }

    pub fn request_response_info(&self) -> Option<u8> {
        self.request_response_info
    }

    pub fn request_problem_info(&self) -> Option<u8> {
        self.request_problem_info
    }

    pub fn user_properties(&self) -> &Vec<(String, String)> {
        &self.user_properties
    }

    pub fn authentication_method(&self) -> &Option<String> {
        &self.authentication_method
    }

    pub fn authentication_data(&self) -> &Option<VarBytes> {
        &self.authentication_data
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        if self.session_expiry_interval.is_some() {
            len += 1 + 4;
        }
        if self.receive_maximum.is_some() {
            len += 1 + 2;
        }
        if self.max_packet_size.is_some() {
            len += 1 + 4;
        }
        if self.topic_alias_max.is_some() {
            len += 1 + 2;
        }
        if self.request_response_info.is_some() {
            len += 1 + 1;
        }
        if self.request_problem_info.is_some() {
            len += 1 + 1;
        }
        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }
        if let Some(authentication_method) = &self.authentication_method {
            len += 1 + authentication_method.encoded_length();
        }
        if let Some(authentication_data) = &self.authentication_data {
            len += 1 + authentication_data.encoded_length();
        }
        self.total_length = VarInt(len)
    }
}

impl Encodable for ConnectProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

        if let Some(session_expiry_interval) = self.session_expiry_interval {
            writer.write_u8(PropertyType::SessionExpiryInterval as u8)?;
            writer.write_u32::<BigEndian>(session_expiry_interval)?;
        }

        if let Some(receive_maximum) = self.receive_maximum {
            writer.write_u8(PropertyType::ReceiveMaximum as u8)?;
            writer.write_u16::<BigEndian>(receive_maximum)?;
        }

        if let Some(max_packet_size) = self.max_packet_size {
            writer.write_u8(PropertyType::MaximumPacketSize as u8)?;
            writer.write_u32::<BigEndian>(max_packet_size)?;
        }

        if let Some(topic_alias_max) = self.topic_alias_max {
            writer.write_u8(PropertyType::TopicAliasMaximum as u8)?;
            writer.write_u16::<BigEndian>(topic_alias_max)?;
        }

        if let Some(request_response_info) = self.request_response_info {
            writer.write_u8(PropertyType::RequestResponseInformation as u8)?;
            writer.write_u8(request_response_info)?;
        }

        if let Some(request_problem_info) = self.request_problem_info {
            writer.write_u8(PropertyType::RequestProblemInformation as u8)?;
            writer.write_u8(request_problem_info)?;
        }

        for (key, value) in self.user_properties.iter() {
            writer.write_u8(PropertyType::UserProperty as u8)?;
            key.encode(writer)?;
            value.encode(writer)?;
        }

        if let Some(authentication_method) = &self.authentication_method {
            writer.write_u8(PropertyType::AuthenticationMethod as u8)?;
            authentication_method.encode(writer)?;
        }

        if let Some(authentication_data) = &self.authentication_data {
            writer.write_u8(PropertyType::AuthenticationData as u8)?;
            authentication_data.encode(writer)?;
        }
        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.total_length.0 + self.total_length.encoded_length()
    }
}

impl Decodable for ConnectProperties {
    type Error = PropertyTypeError;
    type Cond = Option<ConnectFlags>;

    fn decode_with<R: Read>(reader: &mut R, _cond: Self::Cond) -> Result<Self, Self::Error> {
        let mut session_expiry_interval = None;
        let mut receive_maximum = None;
        let mut max_packet_size = None;
        let mut topic_alias_max = None;
        let mut request_response_info = None;
        let mut request_problem_info = None;
        let mut user_properties = Vec::new();
        let mut authentication_method = None;
        let mut authentication_data = None;

        let total_length = VarInt::decode(reader)?;

        if total_length.0 == 0 {
            return Ok(Self::default());
        }

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < total_length.0 {
            let prop = reader.read_u8()?;
            cursor += 1;
            match prop.try_into()? {
                PropertyType::SessionExpiryInterval => {
                    session_expiry_interval = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::ReceiveMaximum => {
                    receive_maximum = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::MaximumPacketSize => {
                    max_packet_size = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::TopicAliasMaximum => {
                    topic_alias_max = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::RequestResponseInformation => {
                    request_response_info = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::RequestProblemInformation => {
                    request_problem_info = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::UserProperty => {
                    let key = String::decode(reader)?;
                    let value = String::decode(reader)?;
                    cursor += 2 + key.len() as u32 + 2 + value.len() as u32;
                    user_properties.push((key, value));
                }
                PropertyType::AuthenticationMethod => {
                    let method = String::decode(reader)?;
                    cursor += 2 + method.len() as u32;
                    authentication_method = Some(method);
                }
                PropertyType::AuthenticationData => {
                    let data = VarBytes::decode(reader)?;
                    cursor += 2 + data.0.len() as u32;
                    authentication_data = Some(data);
                }
                _ => return Err(PropertyTypeError::InvalidPropertyType(prop)),
            }
        }

        Ok(Self {
            total_length,
            session_expiry_interval,
            receive_maximum,
            max_packet_size,
            topic_alias_max,
            request_response_info,
            request_problem_info,
            user_properties,
            authentication_method,
            authentication_data,
        })
    }
}

/// Payloads for connect packet
#[derive(Debug, Eq, PartialEq, Clone)]
struct ConnectPayload {
    client_identifier: String,
    last_will: Option<LastWill>,
    username: Option<String>,
    password: Option<String>,
}

impl ConnectPayload {
    pub fn new(client_identifier: String) -> ConnectPayload {
        ConnectPayload {
            client_identifier,
            last_will: None,
            username: None,
            password: None,
        }
    }
}

impl Encodable for ConnectPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.client_identifier.encode(writer)?;

        if let Some(last_will) = &self.last_will {
            last_will.encode(writer)?;
        }

        if let Some(ref username) = self.username {
            username.encode(writer)?;
        }

        if let Some(ref password) = self.password {
            password.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.client_identifier.encoded_length()
            + self.last_will.encoded_length()
            + self
                .username
                .as_ref()
                .map(|t| t.encoded_length())
                .unwrap_or(0)
            + self
                .password
                .as_ref()
                .map(|t| t.encoded_length())
                .unwrap_or(0)
    }
}

impl Decodable for ConnectPayload {
    type Error = ConnectPacketError;
    type Cond = Option<ConnectFlags>;

    fn decode_with<R: Read>(
        reader: &mut R,
        flags: Option<ConnectFlags>,
    ) -> Result<ConnectPayload, ConnectPacketError> {
        let mut need_will = false;
        let mut need_username = false;
        let mut need_password = false;
        let mut will_retain = false;
        let mut will_qos = 0;

        if let Some(r) = flags {
            need_will = r.will_flag;
            need_username = r.username;
            need_password = r.password;
            will_retain = r.will_retain;
            will_qos = r.will_qos;
        }

        let qos = match will_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => QualityOfService::Level0,
        };

        let identifier = String::decode(reader)?;

        let last_will = if need_will {
            let will_properties = LastWillProperties::decode(reader)?;

            let topic = TopicName::decode(reader).map_err(|e| match e {
                TopicNameDecodeError::IoError(e) => ConnectPacketError::from(e),
                TopicNameDecodeError::InvalidTopicName(e) => e.into(),
            })?;
            let msg = VarBytes::decode(reader)?;
            Some(LastWill {
                topic,
                message: msg,
                qos,
                retain: will_retain,
                properties: will_properties,
            })
        } else {
            None
        };
        let username = if need_username {
            Some(String::decode(reader)?)
        } else {
            None
        };
        let password = if need_password {
            Some(String::decode(reader)?)
        } else {
            None
        };

        Ok(ConnectPayload {
            client_identifier: identifier,
            last_will,
            username,
            password,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ConnectPacketError {
    IoError(#[from] io::Error),
    TopicNameError(#[from] TopicNameError),
    PropertyTypeError(#[from] PropertyTypeError),
}

// LastWill
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LastWill {
    topic: TopicName,
    message: VarBytes,
    qos: QualityOfService,
    retain: bool,
    properties: LastWillProperties,
}

impl LastWill {
    pub fn new<S: Into<String>>(topic: S, msg: Vec<u8>) -> Result<Self, ConnectPacketError> {
        Ok(Self {
            topic: TopicName::new(topic)?,
            message: VarBytes(msg),
            qos: QualityOfService::Level0,
            retain: false,
            properties: LastWillProperties::default(),
        })
    }

    pub fn set_properties(&mut self, properties: LastWillProperties) {
        self.properties = properties;
    }

    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn message(&self) -> &VarBytes {
        &self.message
    }

    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    pub fn retain(&self) -> bool {
        self.retain
    }

    pub fn properties(&self) -> &LastWillProperties {
        &self.properties
    }
}

impl Encodable for LastWill {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.properties.encode(writer)?;
        self.topic.encode(writer)?;
        self.message.encode(writer)?;

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        let mut len = 0;

        len += self.properties.encoded_length();
        len += 2 + self.topic.encoded_length() + 2 + self.message.encoded_length();

        len
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct LastWillProperties {
    total_length: VarInt,
    delay_interval: Option<u32>,
    payload_format_indicator: Option<u8>,
    message_expiry_interval: Option<u32>,
    content_type: Option<String>,
    response_topic: Option<String>,
    correlation_data: Option<VarBytes>,
    user_properties: Vec<(String, String)>,
}

impl LastWillProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn set_delay_interval(&mut self, delay_interval: Option<u32>) {
        self.delay_interval = delay_interval;
        self.fix_total_length();
    }

    pub fn set_payload_format_indicator(&mut self, payload_format_indicator: Option<u8>) {
        self.payload_format_indicator = payload_format_indicator;
        self.fix_total_length();
    }

    pub fn set_message_expiry_interval(&mut self, message_expiry_interval: Option<u32>) {
        self.message_expiry_interval = message_expiry_interval;
        self.fix_total_length();
    }

    pub fn set_content_type(&mut self, content_type: Option<String>) {
        self.content_type = content_type;
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

    pub fn delay_interval(&self) -> Option<u32> {
        self.delay_interval
    }

    pub fn payload_format_indicator(&self) -> Option<u8> {
        self.payload_format_indicator
    }

    pub fn message_expiry_interval(&self) -> Option<u32> {
        self.message_expiry_interval
    }

    pub fn content_type(&self) -> &Option<String> {
        &self.content_type
    }

    pub fn response_topic(&self) -> &Option<String> {
        &self.response_topic
    }

    pub fn correlation_data(&self) -> &Option<VarBytes> {
        &self.correlation_data
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user_properties[..]
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        if self.delay_interval.is_some() {
            len += 1 + 4;
        }
        if self.payload_format_indicator.is_some() {
            len += 1 + 1;
        }
        if self.message_expiry_interval.is_some() {
            len += 1 + 4;
        }
        if let Some(content_type) = &self.content_type {
            len += 1 + content_type.encoded_length();
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
        self.total_length = VarInt(len)
    }
}

impl Encodable for LastWillProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

        if let Some(delay_interval) = self.delay_interval {
            writer.write_u8(PropertyType::WillDelayInterval as u8)?;
            writer.write_u32::<BigEndian>(delay_interval)?;
        }
        if let Some(payload_format_indicator) = self.payload_format_indicator {
            writer.write_u8(PropertyType::PayloadFormatIndicator as u8)?;
            writer.write_u8(payload_format_indicator)?;
        }
        if let Some(message_expiry_interval) = self.message_expiry_interval {
            writer.write_u8(PropertyType::MessageExpiryInterval as u8)?;
            writer.write_u32::<BigEndian>(message_expiry_interval)?;
        }
        if let Some(content_type) = &self.content_type {
            writer.write_u8(PropertyType::ContentType as u8)?;
            content_type.encode(writer)?;
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

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.total_length.0 + self.total_length.encoded_length()
    }
}

impl Decodable for LastWillProperties {
    type Error = ConnectPacketError;
    type Cond = Option<ConnectFlags>;

    fn decode_with<R: Read>(reader: &mut R, _cond: Self::Cond) -> Result<Self, Self::Error> {
        let mut delay_interval = None;
        let mut payload_format_indicator = None;
        let mut message_expiry_interval = None;
        let mut content_type = None;
        let mut response_topic = None;
        let mut correlation_data = None;
        let mut user_properties = Vec::new();

        let total_length = VarInt::decode(reader)?;

        if total_length.0 == 0 {
            return Ok(Self::default());
        }

        let mut cursor = 0;
        // read until cursor reaches property length. properties_len = 0 will skip this loop
        while cursor < total_length.0 {
            let prop = reader.read_u8()?;
            cursor += 1;

            match prop.try_into()? {
                PropertyType::WillDelayInterval => {
                    delay_interval = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::PayloadFormatIndicator => {
                    payload_format_indicator = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::MessageExpiryInterval => {
                    message_expiry_interval = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::ContentType => {
                    let typ = String::decode(reader)?;
                    cursor += 2 + typ.len() as u32;
                    content_type = Some(typ);
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
                _ => return Err(PropertyTypeError::InvalidPropertyType(prop).into()),
            }
        }

        Ok(LastWillProperties {
            total_length,
            delay_interval,
            payload_format_indicator,
            message_expiry_interval,
            content_type,
            response_topic,
            correlation_data,
            user_properties,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    use crate::common::encodable::{Decodable, Encodable};

    #[test]
    fn test_connect_packet_encode_hex() {
        let mut packet = ConnectPacket::new("12345".to_owned());
        packet.set_keep_alive(60);

        let mut props = ConnectProperties::default();
        props.set_session_expiry_interval(Some(4294967295));

        packet.set_properties(props);

        let expected = b"\x10\x17\x00\x04MQTT\x05\x00\x00\x3c\x05\x11\xff\xff\xff\xff\x00\x0512345";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_connect_packet_decode_hex() {
        let encoded_data =
            b"\x10\x17\x00\x04MQTT\x05\x00\x00\x3c\x05\x11\xff\xff\xff\xff\x00\x0512345";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = ConnectPacket::decode(&mut buf).unwrap();

        let mut props = ConnectProperties::default();
        props.set_session_expiry_interval(Some(4294967295));

        let mut expected = ConnectPacket::new("12345".to_owned());
        expected.set_keep_alive(60);
        expected.set_properties(props);

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_connect_packet_basic() {
        let mut packet = ConnectPacket::new("12345".to_owned());
        packet.set_keep_alive(60);

        let mut props = ConnectProperties::default();
        props.set_session_expiry_interval(Some(4294967295));

        packet.set_properties(props);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = ConnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded_packet);
    }

    #[test]
    fn test_connect_packet_username() {
        let mut packet = ConnectPacket::new("12345".to_owned());
        packet.set_username(Some("mqtt_player".to_owned()));

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = ConnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded_packet);
    }

    #[test]
    fn test_connect_packet_username_password() {
        let mut packet = ConnectPacket::new("12345".to_owned());
        packet.set_username(Some("mqtt_player".to_owned()));
        packet.set_password(Some("password".to_string()));

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = ConnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded_packet);
    }

    #[test]
    fn test_connect_packet_with_will_message() {
        let mut packet = ConnectPacket::new("12345".to_owned());

        let mut properties = LastWillProperties::default();
        properties.add_user_property("a", "b");

        let mut last_will = LastWill::new("a/b", b"Hello World".to_vec()).unwrap();
        last_will.set_properties(properties);

        packet.set_will(Some(last_will));
        packet.set_will_qos(2);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = ConnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded_packet);
    }
}
