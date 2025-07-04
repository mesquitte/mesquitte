//! Disconnect Properties

use std::{
    fmt::Display,
    io::{self, Write},
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable, encodable::VarInt},
    v5::property::{PropertyType, PropertyTypeError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DisconnectProperties {
    total_length: VarInt,
    /// Session Expiry Interval in seconds
    session_expiry_interval: Option<u32>,

    /// Human readable reason for the disconnect
    reason_string: Option<String>,

    /// List of user properties
    user_properties: Vec<(String, String)>,

    /// String which can be used by the Client to identify another Server to use.
    server_reference: Option<String>,
}

impl DisconnectProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn set_session_expiry_interval(&mut self, session_expiry_interval: Option<u32>) {
        self.session_expiry_interval = session_expiry_interval;
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

    pub fn set_server_reference(&mut self, server_reference: Option<String>) {
        self.server_reference = server_reference;
        self.fix_total_length();
    }

    pub fn session_expiry_interval(&self) -> Option<u32> {
        self.session_expiry_interval
    }

    pub fn reason_string(&self) -> &Option<String> {
        &self.reason_string
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user_properties[..]
    }

    pub fn server_reference(&self) -> &Option<String> {
        &self.server_reference
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        if self.session_expiry_interval.is_some() {
            len += 1 + 4;
        }
        if let Some(reason_string) = &self.reason_string {
            len += 1 + reason_string.encoded_length();
        }
        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }
        if let Some(server_reference) = &self.server_reference {
            len += 1 + server_reference.encoded_length();
        }
        self.total_length = VarInt(len)
    }
}

impl Encodable for DisconnectProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

        if let Some(session_expiry_interval) = self.session_expiry_interval {
            writer.write_u8(PropertyType::SessionExpiryInterval as u8)?;
            writer.write_u32::<BigEndian>(session_expiry_interval)?;
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
        if let Some(server_reference) = &self.server_reference {
            writer.write_u8(PropertyType::ServerReference as u8)?;
            server_reference.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.total_length.0 + self.total_length.encoded_length()
    }
}

impl Decodable for DisconnectProperties {
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

        let mut session_expiry_interval = None;
        let mut reason_string = None;
        let mut user_properties = Vec::new();
        let mut server_reference = None;

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
                PropertyType::ServerReference => {
                    let reference = String::decode(reader)?;
                    cursor += 2 + reference.len() as u32;
                    server_reference = Some(reference);
                }
                _ => return Err(PropertyTypeError::InvalidPropertyType(prop)),
            }
        }

        Ok(Self {
            total_length,
            session_expiry_interval,
            reason_string,
            user_properties,
            server_reference,
        })
    }
}

impl Display for DisconnectProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        match &self.session_expiry_interval {
            Some(session_expiry_interval) => {
                write!(f, "session_expiry_interval: {session_expiry_interval}")?
            }
            None => write!(f, "session_expiry_interval: None")?,
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
        match &self.server_reference {
            Some(server_reference) => write!(f, ", server_reference: {server_reference}")?,
            None => write!(f, ", server_reference: None")?,
        };
        write!(f, "}}")
    }
}
