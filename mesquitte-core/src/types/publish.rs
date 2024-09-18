use mqtt_codec_kit::common::{QualityOfService, TopicName};
use mqtt_codec_kit::v4::packet::PublishPacket as V4PublishPacket;
use mqtt_codec_kit::v5::{control::PublishProperties, packet::PublishPacket as V5PublishPacket};

#[derive(Clone, Debug)]
pub struct PublishMessage {
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,

    properties: Option<PublishProperties>,
    retain: bool,
    dup: bool,
}

impl PublishMessage {
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn qos(&self) -> &QualityOfService {
        &self.qos
    }

    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
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
}

impl From<(QualityOfService, &V4PublishPacket)> for PublishMessage {
    fn from((qos, packet): (QualityOfService, &V4PublishPacket)) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            qos,
            topic_name: packet.topic_name().to_owned(),
            payload,
            dup: packet.dup(),
            retain: packet.retain(),

            properties: None,
        }
    }
}

impl From<(QualityOfService, &V5PublishPacket)> for PublishMessage {
    fn from((qos, packet): (QualityOfService, &V5PublishPacket)) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            qos,
            topic_name: packet.topic_name().to_owned(),
            payload,
            dup: packet.dup(),
            retain: packet.retain(),

            properties: Some(packet.properties().to_owned()),
        }
    }
}
