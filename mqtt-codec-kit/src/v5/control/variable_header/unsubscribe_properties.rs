//! Unsubscribe Properties

use std::io::{self, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{encodable::VarInt, Decodable, Encodable},
    v5::property::{PropertyType, PropertyTypeError},
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UnsubscribeProperties {
    total_length: VarInt,
    user_properties: Vec<(String, String)>,
}

impl UnsubscribeProperties {
    pub fn is_empty(&self) -> bool {
        self.total_length.0 == 0
    }

    pub fn add_user_property<S: Into<String>>(&mut self, key: S, value: S) {
        self.user_properties.push((key.into(), value.into()));
        self.fix_total_length();
    }

    pub fn user_properties(&self) -> &[(String, String)] {
        &self.user_properties[..]
    }

    #[inline]
    fn fix_total_length(&mut self) {
        let mut len = 0;

        for (key, value) in self.user_properties.iter() {
            len += 1 + key.encoded_length() + value.encoded_length();
        }

        self.total_length = VarInt(len)
    }
}

impl Encodable for UnsubscribeProperties {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.total_length.encode(writer)?;

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

impl Decodable for UnsubscribeProperties {
    type Error = PropertyTypeError;
    type Cond = ();

    fn decode_with<R: std::io::Read>(
        reader: &mut R,
        _cond: Self::Cond,
    ) -> Result<Self, Self::Error> {
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
                PropertyType::UserProperty => {
                    let key = String::decode(reader)?;
                    let value = String::decode(reader)?;
                    cursor += 2 + key.len() as u32 + 2 + value.len() as u32;
                    user_properties.push((key, value));
                }

                _ => return Err(PropertyTypeError::InvalidPropertyType(prop)),
            }
        }

        Ok(UnsubscribeProperties {
            total_length,
            user_properties,
        })
    }
}
