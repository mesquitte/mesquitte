//! Properties
//! <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027>

use std::io;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    PayloadFormatIndicator = 1,
    MessageExpiryInterval = 2,
    ContentType = 3,
    ResponseTopic = 8,
    CorrelationData = 9,
    SubscriptionIdentifier = 11,
    SessionExpiryInterval = 17,
    AssignedClientIdentifier = 18,
    ServerKeepAlive = 19,
    AuthenticationMethod = 21,
    AuthenticationData = 22,
    RequestProblemInformation = 23,
    WillDelayInterval = 24,
    RequestResponseInformation = 25,
    ResponseInformation = 26,
    ServerReference = 28,
    ReasonString = 31,
    ReceiveMaximum = 33,
    TopicAliasMaximum = 34,
    TopicAlias = 35,
    MaximumQos = 36,
    RetainAvailable = 37,
    UserProperty = 38,
    MaximumPacketSize = 39,
    WildcardSubscriptionAvailable = 40,
    SubscriptionIdentifierAvailable = 41,
    SharedSubscriptionAvailable = 42,
}

impl TryFrom<u8> for PropertyType {
    type Error = PropertyTypeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(PropertyType::PayloadFormatIndicator),
            2 => Ok(PropertyType::MessageExpiryInterval),
            3 => Ok(PropertyType::ContentType),
            8 => Ok(PropertyType::ResponseTopic),
            9 => Ok(PropertyType::CorrelationData),
            11 => Ok(PropertyType::SubscriptionIdentifier),
            17 => Ok(PropertyType::SessionExpiryInterval),
            18 => Ok(PropertyType::AssignedClientIdentifier),
            19 => Ok(PropertyType::ServerKeepAlive),
            21 => Ok(PropertyType::AuthenticationMethod),
            22 => Ok(PropertyType::AuthenticationData),
            23 => Ok(PropertyType::RequestProblemInformation),
            24 => Ok(PropertyType::WillDelayInterval),
            25 => Ok(PropertyType::RequestResponseInformation),
            26 => Ok(PropertyType::ResponseInformation),
            28 => Ok(PropertyType::ServerReference),
            31 => Ok(PropertyType::ReasonString),
            33 => Ok(PropertyType::ReceiveMaximum),
            34 => Ok(PropertyType::TopicAliasMaximum),
            35 => Ok(PropertyType::TopicAlias),
            36 => Ok(PropertyType::MaximumQos),
            37 => Ok(PropertyType::RetainAvailable),
            38 => Ok(PropertyType::UserProperty),
            39 => Ok(PropertyType::MaximumPacketSize),
            40 => Ok(PropertyType::WildcardSubscriptionAvailable),
            41 => Ok(PropertyType::SubscriptionIdentifierAvailable),
            42 => Ok(PropertyType::SharedSubscriptionAvailable),
            num => Err(PropertyTypeError::InvalidPropertyType(num)),
        }
    }
}

/// Errors while decoding property type
#[derive(Debug, thiserror::Error)]
pub enum PropertyTypeError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid property type ({0})")]
    InvalidPropertyType(u8),
}
