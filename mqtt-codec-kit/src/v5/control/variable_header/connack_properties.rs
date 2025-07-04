//! Connack Properties

use std::{
    fmt::Display,
    io::{self, Write},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    common::{
        Decodable, Encodable,
        encodable::{VarBytes, VarInt},
    },
    v5::property::{PropertyType, PropertyTypeError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnackProperties {
    total_length: VarInt,
    session_expiry_interval: Option<u32>,
    receive_maximum: Option<u16>,
    max_qos: Option<u8>,
    retain_available: Option<u8>,
    max_packet_size: Option<u32>,
    assigned_client_identifier: Option<String>,
    topic_alias_max: Option<u16>,
    reason_string: Option<String>,
    user_properties: Vec<(String, String)>,
    wildcard_subscription_available: Option<u8>,
    subscription_identifiers_available: Option<u8>,
    shared_subscription_available: Option<u8>,
    server_keep_alive: Option<u16>,
    response_information: Option<String>,
    server_reference: Option<String>,
    authentication_method: Option<String>,
    authentication_data: Option<VarBytes>,
}

impl ConnackProperties {
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

    pub fn set_max_qos(&mut self, max_qos: Option<u8>) {
        self.max_qos = max_qos;
        self.fix_total_length();
    }

    pub fn set_retain_available(&mut self, retain_available: Option<u8>) {
        self.retain_available = retain_available;
        self.fix_total_length();
    }

    pub fn set_max_packet_size(&mut self, max_packet_size: Option<u32>) {
        self.max_packet_size = max_packet_size;
        self.fix_total_length();
    }

    pub fn set_assigned_client_identifier(&mut self, assigned_client_identifier: Option<String>) {
        self.assigned_client_identifier = assigned_client_identifier;
        self.fix_total_length();
    }

    pub fn set_topic_alias_max(&mut self, topic_alias_max: Option<u16>) {
        self.topic_alias_max = topic_alias_max;
        self.fix_total_length();
    }

    pub fn set_reason_string(&mut self, reason_string: Option<String>) {
        self.reason_string = reason_string;
        self.fix_total_length();
    }

    pub fn add_user_property<S: Into<String>>(&mut self, key: S, value: S) {
        self.user_properties.push((key.into(), value.into()));
        self.fix_total_length();
    }

    pub fn set_wildcard_subscription_available(
        &mut self,
        wildcard_subscription_available: Option<u8>,
    ) {
        self.wildcard_subscription_available = wildcard_subscription_available;
        self.fix_total_length();
    }

    pub fn set_subscription_identifiers_available(
        &mut self,
        subscription_identifiers_available: Option<u8>,
    ) {
        self.subscription_identifiers_available = subscription_identifiers_available;
        self.fix_total_length();
    }

    pub fn set_shared_subscription_available(&mut self, shared_subscription_available: Option<u8>) {
        self.shared_subscription_available = shared_subscription_available;
        self.fix_total_length();
    }

    pub fn set_server_keep_alive(&mut self, server_keep_alive: Option<u16>) {
        self.server_keep_alive = server_keep_alive;
        self.fix_total_length();
    }

    pub fn set_response_information(&mut self, response_information: Option<String>) {
        self.response_information = response_information;
        self.fix_total_length();
    }

    pub fn set_server_reference(&mut self, server_reference: Option<String>) {
        self.server_reference = server_reference;
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

    pub fn max_qos(&self) -> Option<u8> {
        self.max_qos
    }

    pub fn retain_available(&self) -> Option<u8> {
        self.retain_available
    }

    pub fn max_packet_size(&self) -> Option<u32> {
        self.max_packet_size
    }

    pub fn assigned_client_identifier(&self) -> &Option<String> {
        &self.assigned_client_identifier
    }

    pub fn topic_alias_max(&self) -> Option<u16> {
        self.topic_alias_max
    }

    pub fn reason_string(&self) -> &Option<String> {
        &self.reason_string
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user_properties[..]
    }

    pub fn wildcard_subscription_available(&self) -> Option<u8> {
        self.wildcard_subscription_available
    }

    pub fn subscription_identifiers_available(&self) -> Option<u8> {
        self.subscription_identifiers_available
    }

    pub fn shared_subscription_available(&self) -> Option<u8> {
        self.shared_subscription_available
    }

    pub fn server_keep_alive(&self) -> Option<u16> {
        self.server_keep_alive
    }

    pub fn response_information(&self) -> &Option<String> {
        &self.response_information
    }

    pub fn server_reference(&self) -> &Option<String> {
        &self.server_reference
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
        if self.max_qos.is_some() {
            len += 1 + 1;
        }
        if self.retain_available.is_some() {
            len += 1 + 1;
        }
        if self.max_packet_size.is_some() {
            len += 1 + 4;
        }
        if let Some(assigned_client_identifier) = &self.assigned_client_identifier {
            len += 1 + assigned_client_identifier.encoded_length();
        }
        if self.topic_alias_max.is_some() {
            len += 1 + 2;
        }
        if let Some(reason_string) = &self.reason_string {
            len += 1 + reason_string.encoded_length();
        }
        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }
        if self.wildcard_subscription_available.is_some() {
            len += 1 + 1;
        }
        if self.subscription_identifiers_available.is_some() {
            len += 1 + 1;
        }
        if self.server_keep_alive.is_some() {
            len += 1 + 2;
        }
        if let Some(response_information) = &self.response_information {
            len += 1 + response_information.encoded_length();
        }
        if let Some(server_reference) = &self.server_reference {
            len += 1 + server_reference.encoded_length();
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

impl Encodable for ConnackProperties {
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
        if let Some(max_qos) = self.max_qos {
            writer.write_u8(PropertyType::MaximumQos as u8)?;
            writer.write_u8(max_qos)?;
        }
        if let Some(retain_available) = self.retain_available {
            writer.write_u8(PropertyType::RetainAvailable as u8)?;
            writer.write_u8(retain_available)?;
        }
        if let Some(max_packet_size) = self.max_packet_size {
            writer.write_u8(PropertyType::MaximumPacketSize as u8)?;
            writer.write_u32::<BigEndian>(max_packet_size)?;
        }
        if let Some(assigned_client_identifier) = &self.assigned_client_identifier {
            writer.write_u8(PropertyType::AssignedClientIdentifier as u8)?;
            assigned_client_identifier.encode(writer)?;
        }
        if let Some(topic_alias_max) = self.topic_alias_max {
            writer.write_u8(PropertyType::TopicAliasMaximum as u8)?;
            writer.write_u16::<BigEndian>(topic_alias_max)?;
        }
        if let Some(reason_string) = &self.reason_string {
            writer.write_u8(PropertyType::ReasonString as u8)?;
            reason_string.encode(writer)?;
        }
        for (key, value) in self.user_properties.iter() {
            writer.write_u8(PropertyType::UserProperty as u8)?;
            key.encode(writer)?;
            value.encode(writer)?;
        }
        if let Some(wildcard_subscription_available) = self.wildcard_subscription_available {
            writer.write_u8(PropertyType::WildcardSubscriptionAvailable as u8)?;
            writer.write_u8(wildcard_subscription_available)?;
        }
        if let Some(subscription_identifiers_available) = self.subscription_identifiers_available {
            writer.write_u8(PropertyType::SubscriptionIdentifierAvailable as u8)?;
            writer.write_u8(subscription_identifiers_available)?;
        }
        if let Some(shared_subscription_available) = self.shared_subscription_available {
            writer.write_u8(PropertyType::SharedSubscriptionAvailable as u8)?;
            writer.write_u8(shared_subscription_available)?;
        }
        if let Some(server_keep_alive) = self.server_keep_alive {
            writer.write_u8(PropertyType::ServerKeepAlive as u8)?;
            writer.write_u16::<BigEndian>(server_keep_alive)?;
        }
        if let Some(response_information) = &self.response_information {
            writer.write_u8(PropertyType::ResponseInformation as u8)?;
            response_information.encode(writer)?;
        }
        if let Some(server_reference) = &self.server_reference {
            writer.write_u8(PropertyType::ServerReference as u8)?;
            server_reference.encode(writer)?;
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

impl Decodable for ConnackProperties {
    type Error = PropertyTypeError;
    type Cond = ();

    fn decode_with<R: std::io::Read>(
        reader: &mut R,
        _cond: Self::Cond,
    ) -> Result<Self, Self::Error> {
        let mut session_expiry_interval = None;
        let mut receive_max = None;
        let mut max_qos = None;
        let mut retain_available = None;
        let mut max_packet_size = None;
        let mut assigned_client_identifier = None;
        let mut topic_alias_max = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut wildcard_subscription_available = None;
        let mut subscription_identifiers_available = None;
        let mut shared_subscription_available = None;
        let mut server_keep_alive = None;
        let mut response_information = None;
        let mut server_reference = None;
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
                    receive_max = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::MaximumQos => {
                    max_qos = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::RetainAvailable => {
                    retain_available = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::AssignedClientIdentifier => {
                    let id = String::decode(reader)?;
                    cursor += 2 + id.len() as u32;
                    assigned_client_identifier = Some(id);
                }
                PropertyType::MaximumPacketSize => {
                    max_packet_size = Some(reader.read_u32::<BigEndian>()?);
                    cursor += 4;
                }
                PropertyType::TopicAliasMaximum => {
                    topic_alias_max = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::ReasonString => {
                    let reason = String::decode(reader)?;
                    cursor += 2 + reason.len() as u32;
                    reason_string = Some(reason);
                }
                PropertyType::UserProperty => {
                    let key = String::decode(reader)?;
                    let value = String::decode(reader)?;
                    cursor += 2 + key.len() as u32 + 2 + value.len() as u32;
                    user_properties.push((key, value));
                }
                PropertyType::WildcardSubscriptionAvailable => {
                    wildcard_subscription_available = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::SubscriptionIdentifierAvailable => {
                    subscription_identifiers_available = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::SharedSubscriptionAvailable => {
                    shared_subscription_available = Some(reader.read_u8()?);
                    cursor += 1;
                }
                PropertyType::ServerKeepAlive => {
                    server_keep_alive = Some(reader.read_u16::<BigEndian>()?);
                    cursor += 2;
                }
                PropertyType::ResponseInformation => {
                    let info = String::decode(reader)?;
                    cursor += 2 + info.len() as u32;
                    response_information = Some(info);
                }
                PropertyType::ServerReference => {
                    let reference = String::decode(reader)?;
                    cursor += 2 + reference.len() as u32;
                    server_reference = Some(reference);
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
            receive_maximum: receive_max,
            max_qos,
            retain_available,
            max_packet_size,
            assigned_client_identifier,
            topic_alias_max,
            reason_string,
            user_properties,
            wildcard_subscription_available,
            subscription_identifiers_available,
            shared_subscription_available,
            server_keep_alive,
            response_information,
            server_reference,
            authentication_method,
            authentication_data,
        })
    }
}

impl Display for ConnackProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        match &self.session_expiry_interval {
            Some(session_expiry_interval) => {
                write!(f, "session_expiry_interval: {session_expiry_interval}")?
            }
            None => write!(f, "session_expiry_interval: None")?,
        };
        match &self.receive_maximum {
            Some(receive_maximum) => write!(f, ", receive_maximum: {receive_maximum}")?,
            None => write!(f, ", receive_maximum: None")?,
        };
        match &self.max_qos {
            Some(max_qos) => write!(f, ", max_qos: {max_qos}")?,
            None => write!(f, ", max_qos: None")?,
        };
        match &self.retain_available {
            Some(retain_available) => write!(f, ", retain_available: {retain_available}")?,
            None => write!(f, ", retain_available: None")?,
        };
        match &self.max_packet_size {
            Some(max_packet_size) => write!(f, ", max_packet_size: {max_packet_size}")?,
            None => write!(f, ", max_packet_size: None")?,
        };
        match &self.assigned_client_identifier {
            Some(assigned_client_identifier) => write!(
                f,
                ", assigned_client_identifier: {assigned_client_identifier}"
            )?,
            None => write!(f, ", assigned_client_identifier: None")?,
        };
        match &self.topic_alias_max {
            Some(topic_alias_max) => write!(f, ", topic_alias_max: {topic_alias_max}")?,
            None => write!(f, ", topic_alias_max: None")?,
        };
        match &self.reason_string {
            Some(reason_string) => write!(f, ", reason_string: {reason_string}")?,
            None => write!(f, ", reason_string: None")?,
        };
        write!(f, ", user_properties: [")?;
        let mut iter = self.user_properties.iter();
        if let Some(first) = iter.next() {
            write!(f, "({}, {})", first.0, first.1)?;
            for property in iter {
                write!(f, ", ({}, {})", property.0, property.1)?;
            }
        }
        write!(f, "]")?;
        match &self.wildcard_subscription_available {
            Some(wildcard_subscription_available) => write!(
                f,
                ", wildcard_subscription_available: {wildcard_subscription_available}"
            )?,
            None => write!(f, ", wildcard_subscription_available: None")?,
        };
        match &self.subscription_identifiers_available {
            Some(subscription_identifiers_available) => write!(
                f,
                ", subscription_identifiers_available: {subscription_identifiers_available}"
            )?,
            None => write!(f, ", subscription_identifiers_available: None")?,
        };
        match &self.shared_subscription_available {
            Some(shared_subscription_available) => write!(
                f,
                ", shared_subscription_available: {shared_subscription_available}"
            )?,
            None => write!(f, ", shared_subscription_available: None")?,
        };
        match &self.server_keep_alive {
            Some(server_keep_alive) => write!(f, ", server_keep_alive: {server_keep_alive}")?,
            None => write!(f, ", server_keep_alive: None")?,
        };
        match &self.response_information {
            Some(response_information) => {
                write!(f, ", response_information: {response_information}")?
            }
            None => write!(f, ", response_information: None")?,
        };
        match &self.server_reference {
            Some(server_reference) => write!(f, ", server_reference: {server_reference}")?,
            None => write!(f, ", server_reference: None")?,
        };
        match &self.authentication_method {
            Some(authentication_method) => {
                write!(f, ", authentication_method: {authentication_method}")?
            }
            None => write!(f, ", authentication_method: None")?,
        };
        match &self.authentication_data {
            Some(authentication_data) => write!(f, ", authentication_data: {authentication_data}")?,
            None => write!(f, ", authentication_data: None")?,
        };
        write!(f, "}}")
    }
}
