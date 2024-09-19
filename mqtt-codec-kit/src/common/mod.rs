pub use self::{
    encodable::{Decodable, Encodable},
    qos::QualityOfService,
    topic_filter::TopicFilter,
    topic_name::{TopicName, TopicNameDecodeError, TopicNameError, TopicNameRef},
    variable_header::*,
};

pub mod encodable;
pub mod packet;
pub mod qos;
pub mod topic_filter;
pub mod topic_name;
pub mod variable_header;

/// Character used to separate each level within a topic tree and provide a hierarchical structure.
pub const LEVEL_SEP: char = '/';
/// Wildcard character that matches only one topic level.
pub const MATCH_ONE_CHAR: char = '+';
/// Wildcard character that matches any number of levels within a topic.
pub const MATCH_ALL_CHAR: char = '#';
/// The &str version of `MATCH_ONE_CHAR`
pub const MATCH_ONE_STR: &str = "+";
/// The &str version of `MATCH_ALL_CHAR`
pub const MATCH_ALL_STR: &str = "#";
/// The &str version of `MATCH_DOLLAR_STR`
pub const MATCH_DOLLAR_STR: &str = "$";

/// System topic prefix
pub const SYS_PREFIX: &str = "$SYS/";
/// Shared topic prefix
pub const SHARED_PREFIX: &str = "$share/";
