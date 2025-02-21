//! Unsuback Properties

use std::{
    fmt::Display,
    io::{self, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable, encodable::VarInt},
    v5::property::{PropertyType, PropertyTypeError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnsubackProperties {
    total_length: VarInt,
    reason_string: Option<String>,
    user_properties: Vec<(String, String)>,
}

impl UnsubackProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn set_reason_string(&mut self, reason_string: Option<String>) {
        self.reason_string = reason_string;
        self.fix_total_length();
    }

    pub fn add_user_property<S: Into<String>>(&mut self, key: S, value: S) {
        self.user_properties.push((key.into(), value.into()));
        self.fix_total_length();
    }

    pub fn reason_string(&self) -> &Option<String> {
        &self.reason_string
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user_properties[..]
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        if let Some(reason_string) = &self.reason_string {
            len += 1 + reason_string.encoded_length();
        }
        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }

        self.total_length = VarInt(len)
    }
}

impl Encodable for UnsubackProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

        if let Some(reason_string) = &self.reason_string {
            writer.write_u8(PropertyType::ReasonString as u8)?;
            reason_string.encode(writer)?;
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

impl Decodable for UnsubackProperties {
    type Error = PropertyTypeError;
    type Cond = ();

    fn decode_with<R: std::io::Read>(
        reader: &mut R,
        _cond: Self::Cond,
    ) -> Result<Self, Self::Error> {
        let mut reason_string = None;
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

                _ => return Err(PropertyTypeError::InvalidPropertyType(prop)),
            }
        }

        Ok(Self {
            total_length,
            reason_string,
            user_properties,
        })
    }
}

impl Display for UnsubackProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        match &self.reason_string {
            Some(reason_string) => write!(f, "reason_string: {}", reason_string)?,
            None => write!(f, "reason_string: None")?,
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
        write!(f, "}}")
    }
}
