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
            1 => Ok(Self::PayloadFormatIndicator),
            2 => Ok(Self::MessageExpiryInterval),
            3 => Ok(Self::ContentType),
            8 => Ok(Self::ResponseTopic),
            9 => Ok(Self::CorrelationData),
            11 => Ok(Self::SubscriptionIdentifier),
            17 => Ok(Self::SessionExpiryInterval),
            18 => Ok(Self::AssignedClientIdentifier),
            19 => Ok(Self::ServerKeepAlive),
            21 => Ok(Self::AuthenticationMethod),
            22 => Ok(Self::AuthenticationData),
            23 => Ok(Self::RequestProblemInformation),
            24 => Ok(Self::WillDelayInterval),
            25 => Ok(Self::RequestResponseInformation),
            26 => Ok(Self::ResponseInformation),
            28 => Ok(Self::ServerReference),
            31 => Ok(Self::ReasonString),
            33 => Ok(Self::ReceiveMaximum),
            34 => Ok(Self::TopicAliasMaximum),
            35 => Ok(Self::TopicAlias),
            36 => Ok(Self::MaximumQos),
            37 => Ok(Self::RetainAvailable),
            38 => Ok(Self::UserProperty),
            39 => Ok(Self::MaximumPacketSize),
            40 => Ok(Self::WildcardSubscriptionAvailable),
            41 => Ok(Self::SubscriptionIdentifierAvailable),
            42 => Ok(Self::SharedSubscriptionAvailable),
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
