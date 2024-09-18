use std::sync::Arc;

use mqtt_codec_kit::common::{QualityOfService, TopicName};
// #[cfg(feature = "v4")]
use mqtt_codec_kit::v4::packet::PublishPacket as V4PublishPacket;
// #[cfg(feature = "v5")]
use mqtt_codec_kit::v5::{control::PublishProperties, packet::PublishPacket as V5PublishPacket};

#[derive(Clone)]
pub struct RetainContent {
    // the publisher client id
    client_identifier: Arc<String>,
    topic_name: TopicName,
    payload: Vec<u8>,
    // #[cfg(feature = "v5")]
    properties: Option<PublishProperties>,
    qos: QualityOfService,
}

impl RetainContent {
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
    }

    pub fn qos(&self) -> &QualityOfService {
        &self.qos
    }

    pub fn client_identifier(&self) -> Arc<String> {
        self.client_identifier.clone()
    }
}

impl From<(Arc<String>, &V4PublishPacket)> for RetainContent {
    fn from((client_identifier, packet): (Arc<String>, &V4PublishPacket)) -> Self {
        Self {
            client_identifier,
            topic_name: packet.topic_name().clone(),
            payload: packet.payload().into(),
            qos: packet.qos().into(),

            properties: None,
        }
    }
}

impl From<(Arc<String>, &V5PublishPacket)> for RetainContent {
    fn from((client_identifier, packet): (Arc<String>, &V5PublishPacket)) -> Self {
        Self {
            client_identifier,
            topic_name: packet.topic_name().clone(),
            payload: packet.payload().into(),
            qos: packet.qos().into(),

            properties: Some(packet.properties().clone()),
        }
    }
}
