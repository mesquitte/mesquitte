//! Variable header in MQTT

use std::{io, string::FromUtf8Error};

use crate::{
    common::{
        protocol_level::ProtocolLevelError,
        topic_name::{TopicNameDecodeError, TopicNameError},
    },
    v5::property::PropertyTypeError,
};

pub use self::{
    auth_properties::AuthProperties, authenticate_reason_code::AuthenticateReasonCode,
    connack_properties::ConnackProperties, connect_reason_code::ConnectReasonCode,
    disconnect_properties::DisconnectProperties, disconnect_reason_code::DisconnectReasonCode,
    puback_properties::PubackProperties, puback_reason_code::PubackReasonCode,
    pubcomp_properties::PubcompProperties, pubcomp_reason_code::PubcompReasonCode,
    publish_properties::PublishProperties, pubrec_properties::PubrecProperties,
    pubrec_reason_code::PubrecReasonCode, pubrel_properties::PubrelProperties,
    pubrel_reason_code::PubrelReasonCode, suback_properties::SubackProperties,
    subscribe_properties::SubscribeProperties, unsuback_properties::UnsubackProperties,
    unsubscribe_properties::UnsubscribeProperties,
};

mod auth_properties;
mod authenticate_reason_code;
mod connack_properties;
mod connect_reason_code;
mod disconnect_properties;
mod disconnect_reason_code;
mod puback_properties;
mod puback_reason_code;
mod pubcomp_properties;
mod pubcomp_reason_code;
mod publish_properties;
mod pubrec_properties;
mod pubrec_reason_code;
mod pubrel_properties;
mod pubrel_reason_code;
mod suback_properties;
mod subscribe_properties;
mod unsuback_properties;
mod unsubscribe_properties;

/// Errors while decoding variable header
#[derive(Debug, thiserror::Error)]
pub enum VariableHeaderError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid remaining length")]
    InvalidRemainingLength,
    #[error("invalid reserved flags")]
    InvalidReservedFlag,
    #[error(transparent)]
    FromUtf8Error(#[from] FromUtf8Error),
    #[error(transparent)]
    TopicNameError(#[from] TopicNameError),
    #[error(transparent)]
    InvalidProtocolLevel(#[from] ProtocolLevelError),
    #[error("invalid connect reason code ({0})")]
    InvalidConnectReasonCode(u8),
    #[error("invalid puback reason code ({0})")]
    InvalidPubackReasonCode(u8),
    #[error("invalid pubrec reason code ({0})")]
    InvalidPubrecReasonCode(u8),
    #[error("invalid pubrel reason code ({0})")]
    InvalidPubrelReasonCode(u8),
    #[error("invalid pubcomp reason code ({0})")]
    InvalidPubcompReasonCode(u8),
    #[error("invalid unsubscribe reason code ({0})")]
    InvalidUnsubscribeReasonCode(u8),
    #[error("invalid disconnect reason code ({0})")]
    InvalidDisconnectReasonCode(u8),
    #[error("invalid authenticate reason code ({0})")]
    InvalidAuthenticateReasonCode(u8),
    #[error(transparent)]
    PropertyTypeError(#[from] PropertyTypeError),
}

impl From<TopicNameDecodeError> for VariableHeaderError {
    fn from(err: TopicNameDecodeError) -> VariableHeaderError {
        match err {
            TopicNameDecodeError::IoError(e) => Self::IoError(e),
            TopicNameDecodeError::InvalidTopicName(e) => Self::TopicNameError(e),
        }
    }
}
