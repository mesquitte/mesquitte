use std::{fmt::Debug, future::Future, io, time::SystemTime};

use mqtt_codec_kit::common::{QualityOfService, TopicName, qos::QoSWithPacketIdentifier};
#[cfg(feature = "v4")]
use mqtt_codec_kit::v4::{
    packet::PublishPacket as V4PublishPacket, packet::connect::LastWill as V4LastWill,
};
#[cfg(feature = "v5")]
use mqtt_codec_kit::v5::{
    control::PublishProperties, packet::PublishPacket as V5PublishPacket,
    packet::connect::LastWill as V5LastWill,
};

pub fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[derive(Clone, Debug)]
pub struct PublishMessage {
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,
    retain: bool,
    dup: bool,
    #[cfg(feature = "v5")]
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

    pub fn retain(&self) -> bool {
        self.retain
    }

    pub fn set_retain(&mut self, retain: bool) {
        self.retain = retain
    }

    #[cfg(feature = "v5")]
    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
    }
}

#[cfg(feature = "v4")]
impl From<V4PublishPacket> for PublishMessage {
    fn from(packet: V4PublishPacket) -> Self {
        let mut payload = vec![0u8; packet.payload().len()];
        payload.copy_from_slice(packet.payload());

        Self {
            topic_name: packet.topic_name().to_owned(),
            payload,
            qos: packet.qos().into(),
            retain: packet.retain(),
            dup: packet.dup(),
            #[cfg(feature = "v5")]
            properties: None,
        }
    }
}

#[cfg(feature = "v5")]
impl From<V5PublishPacket> for PublishMessage {
    fn from(packet: V5PublishPacket) -> Self {
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

#[cfg(feature = "v4")]
impl From<V4LastWill> for PublishMessage {
    fn from(value: V4LastWill) -> Self {
        let mut payload = vec![0u8; value.message().0.len()];
        payload.copy_from_slice(&value.message().0);

        Self {
            topic_name: value.topic().to_owned(),
            payload,
            qos: value.qos(),
            retain: value.retain(),
            dup: false,
            #[cfg(feature = "v5")]
            properties: None,
        }
    }
}

#[cfg(feature = "v5")]
impl From<V5LastWill> for PublishMessage {
    fn from(value: V5LastWill) -> Self {
        let mut payload = vec![0u8; value.message().0.len()];
        payload.copy_from_slice(&value.message().0);

        let mut publish_properties = PublishProperties::default();
        let properties = value.properties();
        publish_properties.set_payload_format_indicator(properties.payload_format_indicator());
        publish_properties.set_message_expiry_interval(properties.message_expiry_interval());
        publish_properties.set_response_topic(properties.response_topic().clone());
        publish_properties.set_correlation_data(properties.correlation_data().clone().map(|v| v.0));
        for (key, value) in properties.user_properties() {
            publish_properties.add_user_property(key, value);
        }
        publish_properties.set_content_type(properties.content_type().clone());

        Self {
            topic_name: value.topic().to_owned(),
            payload,
            qos: value.qos(),
            retain: value.retain(),
            dup: false,
            properties: Some(publish_properties),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PendingPublishMessage {
    message: PublishMessage,
    qos: QoSWithPacketIdentifier,
    dup: bool,
    pubrec_at: Option<u64>,
}

impl PendingPublishMessage {
    pub fn new(qos: QoSWithPacketIdentifier, message: PublishMessage) -> Self {
        Self {
            pubrec_at: None,
            qos,
            dup: message.dup,
            message,
        }
    }

    pub fn pubrec_at(&self) -> Option<u64> {
        self.pubrec_at
    }

    pub fn renew_pubrec_at(&mut self) {
        self.pubrec_at = Some(get_unix_ts())
    }

    pub fn message(&self) -> &PublishMessage {
        &self.message
    }

    pub fn dup(&self) -> bool {
        self.dup
    }

    pub fn set_dup(&mut self, dup: bool) {
        self.dup = dup;
    }

    pub fn qos(&self) -> QoSWithPacketIdentifier {
        self.qos
    }
}

pub trait MessageStore: Send + Sync {
    fn save_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<Option<PublishMessage>, io::Error>> + Send;

    fn save_pending_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PendingPublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn try_get_pending_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<Option<Vec<(u16, PendingPublishMessage)>>, io::Error>> + Send;

    fn get_all_pending_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<Option<Vec<(u16, PendingPublishMessage)>>, io::Error>> + Send;

    fn puback(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn pubrec(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn pubcomp(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn is_full(&self, client_id: &str) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn message_count(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<usize, io::Error>> + Send;

    fn clear_all(&self, client_id: &str) -> impl Future<Output = Result<(), io::Error>> + Send;
}
