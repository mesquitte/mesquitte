//! Topic filter

use std::{
    fmt::Display,
    io::{self, Read, Write},
    ops::Deref,
};

use crate::common::{
    TopicNameRef, {Decodable, Encodable},
};

use super::{SHARED_PREFIX, SYS_PREFIX};

/// return (shared group name, shared filter)
fn topic_filter_shared_info(topic: &str) -> Option<(&str, &str)> {
    if topic.starts_with(SHARED_PREFIX) {
        let start_index = SHARED_PREFIX.len();

        if let Some(end_index) = topic[start_index..].find('/') {
            let end = start_index + end_index;
            return Some((&topic[start_index..end], &topic[end + 1..]));
        }
    }

    None
}

#[inline]
fn is_invalid_topic_filter(topic: &str) -> bool {
    if topic.is_empty() || topic.len() > 65535 {
        return true;
    }

    // TODO: starts_with('#')
    // if topic.starts_with('#') {
    //     return true;
    // }

    let topic = if let Some((_, topic)) = topic_filter_shared_info(topic) {
        topic
    } else {
        topic
    };

    let mut found_hash = false;
    for member in topic.split('/') {
        if found_hash {
            return true;
        }

        match member {
            "#" => found_hash = true,
            "+" => {}
            _ => {
                if member.contains(['#', '+']) {
                    return true;
                }
            }
        }
    }

    false
}

/// Topic filter
///
/// <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106>
///
/// ```rust
/// use mqtt_codec_kit::common::{TopicFilter, TopicNameRef};
///
/// let topic_filter = TopicFilter::new("sport/+/player1").unwrap();
/// let matcher = topic_filter.get_matcher();
/// assert!(matcher.is_match(TopicNameRef::new("sport/abc/player1").unwrap()));
/// ```
#[derive(Debug, Eq, PartialEq, Clone, Hash, Ord, PartialOrd)]
pub struct TopicFilter(String);

impl TopicFilter {
    /// Creates a new topic filter from string
    /// Return error if it is not a valid topic filter
    pub fn new<S: Into<String>>(topic: S) -> Result<Self, TopicFilterError> {
        let topic = topic.into();
        if is_invalid_topic_filter(&topic) {
            Err(TopicFilterError(topic))
        } else {
            Ok(Self(topic))
        }
    }

    /// Creates a new topic filter from string without validation
    ///
    /// # Safety
    ///
    /// Topic filters' syntax is defined in [MQTT specification](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106).
    /// Creating a filter from raw string may cause errors
    pub unsafe fn new_unchecked<S: Into<String>>(topic: S) -> Self {
        Self(topic.into())
    }

    pub fn is_shared(&self) -> bool {
        topic_filter_shared_info(&self.0).is_some()
    }

    pub fn is_sys(&self) -> bool {
        self.0.starts_with(SYS_PREFIX)
    }

    pub fn shared_group_name(&self) -> Option<&str> {
        if let Some((group_name, _)) = topic_filter_shared_info(&self.0) {
            Some(group_name)
        } else {
            None
        }
    }

    /// return (shared group name, shared filter)
    pub fn shared_info(&self) -> Option<(&str, &str)> {
        topic_filter_shared_info(&self.0)
    }
}

impl From<TopicFilter> for String {
    fn from(topic: TopicFilter) -> Self {
        topic.0
    }
}

impl Encodable for TopicFilter {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        (&self.0[..]).encode(writer)
    }

    fn encoded_length(&self) -> u32 {
        (&self.0[..]).encoded_length()
    }
}

impl Decodable for TopicFilter {
    type Error = TopicFilterDecodeError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        let topic_filter = String::decode(reader)?;
        Ok(Self::new(topic_filter)?)
    }
}

impl Deref for TopicFilter {
    type Target = TopicFilterRef;

    fn deref(&self) -> &Self::Target {
        unsafe { TopicFilterRef::new_unchecked(&self.0) }
    }
}

impl Display for TopicFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Reference to a `TopicFilter`
#[derive(Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct TopicFilterRef(str);

impl TopicFilterRef {
    /// Creates a new topic filter from string
    /// Return error if it is not a valid topic filter
    pub fn new<S: AsRef<str> + ?Sized>(topic: &S) -> Result<&Self, TopicFilterError> {
        let topic = topic.as_ref();
        if is_invalid_topic_filter(topic) {
            Err(TopicFilterError(topic.to_owned()))
        } else {
            Ok(unsafe { &*(topic as *const str as *const Self) })
        }
    }

    /// Creates a new topic filter from string without validation
    ///
    /// # Safety
    ///
    /// Topic filters' syntax is defined in [MQTT specification](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718106).
    /// Creating a filter from raw string may cause errors
    pub unsafe fn new_unchecked<S: AsRef<str> + ?Sized>(topic: &S) -> &Self {
        let topic = topic.as_ref();
        &*(topic as *const str as *const Self)
    }

    /// Get a matcher
    pub fn get_matcher(&self) -> TopicFilterMatcher<'_> {
        TopicFilterMatcher::new(&self.0)
    }
}

impl Deref for TopicFilterRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid topic filter ({0})")]
pub struct TopicFilterError(pub String);

/// Errors while parsing topic filters
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum TopicFilterDecodeError {
    IoError(#[from] io::Error),
    InvalidTopicFilter(#[from] TopicFilterError),
}

/// Matcher for matching topic names with this filter
#[derive(Debug, Copy, Clone)]
pub struct TopicFilterMatcher<'a> {
    topic_filter: &'a str,
}

impl<'a> TopicFilterMatcher<'a> {
    fn new(filter: &'a str) -> TopicFilterMatcher<'a> {
        TopicFilterMatcher {
            topic_filter: filter,
        }
    }

    /// Check if this filter can match the `topic_name`
    pub fn is_match(&self, topic_name: &TopicNameRef) -> bool {
        let mut tn_itr = topic_name.split('/');
        let mut ft_itr = self.topic_filter.split('/');

        // The Server MUST NOT match Topic Filters starting with a wildcard character (# or +)
        // with Topic Names beginning with a $ character [MQTT-4.7.2-1].

        let first_ft = ft_itr.next().unwrap();
        let first_tn = tn_itr.next().unwrap();

        if first_tn.starts_with('$') {
            if first_tn != first_ft {
                return false;
            }
        } else {
            match first_ft {
                // Matches the whole topic
                "#" => return true,
                "+" => {}
                _ => {
                    if first_tn != first_ft {
                        return false;
                    }
                }
            }
        }

        loop {
            match (ft_itr.next(), tn_itr.next()) {
                (Some(ft), Some(tn)) => match ft {
                    "#" => break,
                    "+" => {}
                    _ => {
                        if ft != tn {
                            return false;
                        }
                    }
                },
                (Some(ft), None) => {
                    if ft != "#" {
                        return false;
                    } else {
                        break;
                    }
                }
                (None, Some(..)) => return false,
                (None, None) => break,
            }
        }

        true
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn topic_filter_validate() {
        let topic = "#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport/tennis/player1".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport/tennis/player1/ranking".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport/tennis/player1/#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport/tennis/#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport/tennis#".to_owned();
        assert!(TopicFilter::new(topic).is_err());

        let topic = "sport/tennis/#/ranking".to_owned();
        assert!(TopicFilter::new(topic).is_err());

        let topic = "+".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "+/tennis/#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "sport+".to_owned();
        assert!(TopicFilter::new(topic).is_err());

        let topic = "sport/+/player1".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "+/+".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "$SYS/#".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "$SYS".to_owned();
        TopicFilter::new(topic).unwrap();

        let topic = "$share/".to_owned();
        let t = TopicFilter::new(topic).unwrap();
        println!("{}", t.is_shared())
    }

    #[test]
    fn topic_filter_matcher() {
        let filter = TopicFilter::new("sport/#").unwrap();
        let matcher = filter.get_matcher();
        assert!(matcher.is_match(TopicNameRef::new("sport").unwrap()));

        let filter = TopicFilter::new("#").unwrap();
        let matcher = filter.get_matcher();
        assert!(matcher.is_match(TopicNameRef::new("sport").unwrap()));
        assert!(matcher.is_match(TopicNameRef::new("/").unwrap()));
        assert!(matcher.is_match(TopicNameRef::new("abc/def").unwrap()));
        assert!(!matcher.is_match(TopicNameRef::new("$SYS").unwrap()));
        assert!(!matcher.is_match(TopicNameRef::new("$SYS/abc").unwrap()));

        let filter = TopicFilter::new("+/monitor/Clients").unwrap();
        let matcher = filter.get_matcher();
        assert!(!matcher.is_match(TopicNameRef::new("$SYS/monitor/Clients").unwrap()));

        let filter = TopicFilter::new("$SYS/#").unwrap();
        let matcher = filter.get_matcher();
        assert!(matcher.is_match(TopicNameRef::new("$SYS/monitor/Clients").unwrap()));
        assert!(matcher.is_match(TopicNameRef::new("$SYS").unwrap()));

        let filter = TopicFilter::new("$SYS/monitor/+").unwrap();
        let matcher = filter.get_matcher();
        assert!(matcher.is_match(TopicNameRef::new("$SYS/monitor/Clients").unwrap()));
    }
}
