//! Variable header in MQTT
use std::{io, string::FromUtf8Error};

use crate::common::{
    protocol_level::ProtocolLevelError,
    topic_name::{TopicNameDecodeError, TopicNameError},
};

pub use self::connect_ret_code::ConnectReturnCode;

mod connect_ret_code;

/// Errors while decoding variable header
#[derive(Debug, thiserror::Error)]
pub enum VariableHeaderError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid reserved flags")]
    InvalidReservedFlag,
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error(transparent)]
    TopicNameError(#[from] TopicNameError),
    #[error(transparent)]
    InvalidProtocolLevel(#[from] ProtocolLevelError),
}

impl From<TopicNameDecodeError> for VariableHeaderError {
    fn from(err: TopicNameDecodeError) -> Self {
        match err {
            TopicNameDecodeError::IoError(e) => VariableHeaderError::IoError(e),
            TopicNameDecodeError::InvalidTopicName(e) => VariableHeaderError::TopicNameError(e),
        }
    }
}
