use std::sync::Arc;

use mqtt_codec_kit::common::{QualityOfService, TopicName};
use mqtt_codec_kit::v4::packet::PublishPacket as V4PublishPacket;
use mqtt_codec_kit::v5::{control::PublishProperties, packet::PublishPacket as V5PublishPacket};

use super::last_will::LastWill;
use super::retain_content::RetainContent;

#[derive(Clone, Debug)]
pub struct PublishMessage {
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,
    retain: bool,
    dup: bool,
    properties: Option<PublishProperties>,
}

impl PublishMessage {
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    pub fn dup(&self) -> bool {
        self.dup
    }

    pub fn set_dup(&mut self) {
        self.dup = true
    }

    pub fn retain(&self) -> bool {
        self.retain
    }

    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
    }
}

impl From<&V4PublishPacket> for PublishMessage {
    fn from(packet: &V4PublishPacket) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            topic_name: packet.topic_name().to_owned(),
            payload,
            qos: packet.qos().into(),
            retain: packet.retain(),
            dup: packet.dup(),
            properties: None,
        }
    }
}

impl From<&V5PublishPacket> for PublishMessage {
    fn from(packet: &V5PublishPacket) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            topic_name: packet.topic_name().to_owned(),
            payload,
            qos: packet.qos().into(),
            retain: packet.retain(),
            dup: packet.dup(),
            properties: Some(packet.properties().to_owned()),
        }
    }
}

impl From<LastWill> for PublishMessage {
    fn from(packet: LastWill) -> Self {
        let mut payload = vec![0u8; packet.message().len()];
        payload.copy_from_slice(packet.message());

        Self {
            topic_name: packet.topic_name().to_owned(),
            payload,
            qos: packet.qos(),
            retain: packet.retain(),
            dup: false,
            properties: packet.properties().map(|properties| {
                let mut publish_properties = PublishProperties::default();
                publish_properties
                    .set_payload_format_indicator(properties.payload_format_indicator());
                publish_properties
                    .set_message_expiry_interval(properties.message_expiry_interval());
                publish_properties.set_response_topic(properties.response_topic().clone());
                publish_properties
                    .set_correlation_data(properties.correlation_data().clone().map(|v| v.0));
                for (key, value) in properties.user_properties() {
                    publish_properties.add_user_property(key, value);
                }
                publish_properties.set_content_type(properties.content_type().clone());

                publish_properties
            }),
        }
    }
}

impl From<Arc<RetainContent>> for PublishMessage {
    fn from(packet: Arc<RetainContent>) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            topic_name: packet.topic_name().to_owned(),
            payload,
            qos: packet.qos().to_owned(),
            retain: false,
            dup: false,
            properties: packet.properties().cloned(),
        }
    }
}
