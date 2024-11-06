//! Topic name

use std::{
    borrow::{Borrow, BorrowMut},
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    str::FromStr,
};

use crate::common::{Decodable, Encodable};

#[inline]
fn is_invalid_topic_name(topic_name: &str) -> bool {
    topic_name.is_empty()
        || topic_name.as_bytes().len() > 65535
        || topic_name.chars().any(|ch| ch == '#' || ch == '+')
}

/// Topic name
///
/// [MQTT v3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106)
/// [MQTT v5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241)
#[derive(Debug, Eq, PartialEq, Clone, Hash, Ord, PartialOrd)]
pub struct TopicName(String);

impl TopicName {
    /// Creates a new topic name from string
    /// Return error if the string is not a valid topic name
    pub fn new<S: Into<String>>(topic_name: S) -> Result<Self, TopicNameError> {
        let topic_name = topic_name.into();
        if is_invalid_topic_name(&topic_name) {
            Err(TopicNameError(topic_name))
        } else {
            Ok(Self(topic_name))
        }
    }

    /// Creates a new topic name from string without validation
    ///
    /// # Safety
    ///
    /// Topic names' syntax is defined in
    /// [MQTT v3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106)
    /// [MQTT v5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241)
    /// Creating a name from raw string may cause errors
    pub unsafe fn new_unchecked(topic_name: String) -> Self {
        Self(topic_name)
    }
}

impl From<TopicName> for String {
    fn from(topic_name: TopicName) -> Self {
        topic_name.0
    }
}

impl FromStr for TopicName {
    type Err = TopicNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TopicName::new(s)
    }
}

impl Deref for TopicName {
    type Target = TopicNameRef;

    fn deref(&self) -> &Self::Target {
        unsafe { TopicNameRef::new_unchecked(&self.0) }
    }
}

impl DerefMut for TopicName {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { TopicNameRef::new_mut_unchecked(&mut self.0) }
    }
}

impl Borrow<TopicNameRef> for TopicName {
    fn borrow(&self) -> &TopicNameRef {
        Deref::deref(self)
    }
}

impl BorrowMut<TopicNameRef> for TopicName {
    fn borrow_mut(&mut self) -> &mut TopicNameRef {
        DerefMut::deref_mut(self)
    }
}

impl Encodable for TopicName {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self.0[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self.0[..]).encoded_length()
    }
}

impl Decodable for TopicName {
    type Error = TopicNameDecodeError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        let topic_name = String::decode(reader)?;
        Ok(Self::new(topic_name)?)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid topic filter ({0})")]
pub struct TopicNameError(pub String);

/// Errors while parsing topic names
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum TopicNameDecodeError {
    IoError(#[from] io::Error),
    InvalidTopicName(#[from] TopicNameError),
}

/// Reference to a topic name
#[derive(Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct TopicNameRef(str);

impl TopicNameRef {
    /// Creates a new topic name from string
    /// Return error if the string is not a valid topic name
    pub fn new<S: AsRef<str> + ?Sized>(topic_name: &S) -> Result<&Self, TopicNameError> {
        let topic_name = topic_name.as_ref();
        if is_invalid_topic_name(topic_name) {
            Err(TopicNameError(topic_name.to_owned()))
        } else {
            Ok(unsafe { &*(topic_name as *const str as *const Self) })
        }
    }

    /// Creates a new topic name from string
    /// Return error if the string is not a valid topic name
    pub fn new_mut<S: AsMut<str> + ?Sized>(
        topic_name: &mut S,
    ) -> Result<&mut Self, TopicNameError> {
        let topic_name = topic_name.as_mut();
        if is_invalid_topic_name(topic_name) {
            Err(TopicNameError(topic_name.to_owned()))
        } else {
            Ok(unsafe { &mut *(topic_name as *mut str as *mut Self) })
        }
    }

    /// Creates a new topic name from string without validation
    ///
    /// # Safety
    ///
    /// Topic names' syntax is defined in
    /// [MQTT v3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106)
    /// [MQTT v5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241)
    /// Creating a name from raw string may cause errors
    pub unsafe fn new_unchecked<S: AsRef<str> + ?Sized>(topic_name: &S) -> &Self {
        let topic_name = topic_name.as_ref();
        &*(topic_name as *const str as *const Self)
    }

    /// Creates a new topic name from string without validation
    ///
    /// # Safety
    ///
    /// Topic names' syntax is defined in
    /// [MQTT v3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106)
    /// [MQTT v5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241)
    /// Creating a name from raw string may cause errors
    pub unsafe fn new_mut_unchecked<S: AsMut<str> + ?Sized>(topic_name: &mut S) -> &mut Self {
        let topic_name = topic_name.as_mut();
        &mut *(topic_name as *mut str as *mut Self)
    }

    /// Check if this topic name is only for server.
    ///
    /// Topic names that beginning with a '$' character are reserved for servers
    pub fn is_server_specific(&self) -> bool {
        self.0.starts_with('$')
    }
}

impl Deref for TopicNameRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ToOwned for TopicNameRef {
    type Owned = TopicName;

    fn to_owned(&self) -> Self::Owned {
        TopicName(self.0.to_owned())
    }
}

impl Encodable for TopicNameRef {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self.0[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self.0[..]).encoded_length()
    }
}

/// Topic name wrapper
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TopicNameHeader(TopicName);

impl TopicNameHeader {
    pub fn new(topic_name: String) -> Result<Self, TopicNameDecodeError> {
        match TopicName::new(topic_name) {
            Ok(h) => Ok(Self(h)),
            Err(err) => Err(TopicNameDecodeError::InvalidTopicName(err)),
        }
    }
}

impl From<TopicNameHeader> for TopicName {
    fn from(hdr: TopicNameHeader) -> Self {
        hdr.0
    }
}

impl Encodable for TopicNameHeader {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self.0[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self.0[..]).encoded_length()
    }
}

impl Decodable for TopicNameHeader {
    type Error = TopicNameDecodeError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        Self::new(Decodable::decode(reader)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn topic_name_sys() {
        let topic_name = "$SYS".to_owned();
        TopicName::new(topic_name).unwrap();

        let topic_name = "$SYS/broker/connection/test.cosm-energy/state".to_owned();
        TopicName::new(topic_name).unwrap();
    }

    #[test]
    fn topic_name_slash() {
        TopicName::new("/").unwrap();
    }

    #[test]
    fn topic_name_basic() {
        TopicName::new("/finance").unwrap();
        TopicName::new("/finance//def").unwrap();
    }
}
