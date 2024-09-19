use mqtt_codec_kit::common::{QualityOfService, TopicName};
// #[cfg(feature = "v4")]
use mqtt_codec_kit::v4::packet::connect::LastWill as V4LastWill;
// #[cfg(feature = "v5")]
use mqtt_codec_kit::v5::packet::connect::{LastWill as V5LastWill, LastWillProperties};

#[derive(Debug)]
pub struct LastWill {
    topic_name: TopicName,
    message: Vec<u8>,
    qos: QualityOfService,
    retain: bool,

    // TODO: v5
    // #[cfg(feature = "v5")]
    properties: Option<LastWillProperties>,
}

impl LastWill {
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn message(&self) -> &[u8] {
        &self.message
    }

    pub fn retain(&self) -> bool {
        self.retain
    }

    pub fn properties(&self) -> Option<&LastWillProperties> {
        self.properties.as_ref()
    }

    pub fn qos(&self) -> &QualityOfService {
        &self.qos
    }
}

// #[cfg(feature = "v4")]
impl From<V4LastWill> for LastWill {
    fn from(value: V4LastWill) -> Self {
        let mut message = vec![0u8; value.message().0.len()];
        message.copy_from_slice(&value.message().0);

        Self {
            topic_name: value.topic().to_owned(),
            message,
            qos: value.qos(),
            retain: value.retain(),
            properties: None,
        }
    }
}

// #[cfg(feature = "v5")]
impl From<V5LastWill> for LastWill {
    fn from(value: V5LastWill) -> Self {
        let mut message = vec![0u8; value.message().0.len()];
        message.copy_from_slice(&value.message().0);

        Self {
            topic_name: value.topic().to_owned(),
            message,
            qos: value.qos(),
            retain: value.retain(),
            properties: Some(value.properties().to_owned()),
        }
    }
}
