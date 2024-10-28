use std::{cmp, fmt::Debug, future::Future, io, sync::Arc, time::SystemTime};

use mqtt_codec_kit::common::{QualityOfService, TopicName};
// #[cfg(feature = "v4")]
use mqtt_codec_kit::v4::{
    packet::connect::LastWill as V4LastWill, packet::PublishPacket as V4PublishPacket,
};
// #[cfg(feature = "v5")]
use mqtt_codec_kit::v5::{
    control::PublishProperties, packet::connect::LastWill as V5LastWill,
    packet::PublishPacket as V5PublishPacket,
};

use super::retain::RetainContent;

pub fn get_unix_ts() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[derive(Clone, Debug)]
pub struct ReceivedPublishMessage {
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,
    retain: bool,
    dup: bool,
    properties: Option<PublishProperties>,
    receive_at: u64,
}

impl ReceivedPublishMessage {
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

    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
    }

    pub fn receive_at(&self) -> u64 {
        self.receive_at
    }
}

impl From<&V4PublishPacket> for ReceivedPublishMessage {
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
            receive_at: get_unix_ts(),
        }
    }
}

impl From<&V5PublishPacket> for ReceivedPublishMessage {
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
            receive_at: get_unix_ts(),
        }
    }
}

impl From<Arc<RetainContent>> for ReceivedPublishMessage {
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
            receive_at: get_unix_ts(),
        }
    }
}

// #[cfg(feature = "v4")]
impl From<V4LastWill> for ReceivedPublishMessage {
    fn from(value: V4LastWill) -> Self {
        let mut payload = vec![0u8; value.message().0.len()];
        payload.copy_from_slice(&value.message().0);

        Self {
            topic_name: value.topic().to_owned(),
            payload,
            qos: value.qos(),
            retain: value.retain(),
            properties: None,
            dup: false,
            receive_at: get_unix_ts(),
        }
    }
}

// #[cfg(feature = "v5")]
impl From<V5LastWill> for ReceivedPublishMessage {
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
            receive_at: get_unix_ts(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PendingPublishMessage {
    server_packet_id: u16,
    subscribe_qos: QualityOfService,
    message: ReceivedPublishMessage,
    dup: bool,
    receive_at: u64,
    pubrec_at: Option<u64>,
}

impl PendingPublishMessage {
    pub fn new(
        server_packet_id: u16,
        subscribe_qos: QualityOfService,
        message: ReceivedPublishMessage,
    ) -> Self {
        Self {
            server_packet_id,
            receive_at: get_unix_ts(),
            pubrec_at: None,
            subscribe_qos,
            dup: message.dup,
            message,
        }
    }

    pub fn server_packet_id(&self) -> u16 {
        self.server_packet_id
    }

    pub fn receive_at(&self) -> u64 {
        self.receive_at
    }

    pub fn pubrec_at(&self) -> Option<u64> {
        self.pubrec_at
    }

    pub fn renew_pubrec_at(&mut self) {
        self.pubrec_at = Some(get_unix_ts())
    }

    pub fn message(&self) -> &ReceivedPublishMessage {
        &self.message
    }

    pub fn dup(&self) -> bool {
        self.dup
    }

    pub fn set_dup(&mut self) {
        self.dup = true;
    }

    pub fn subscribe_qos(&self) -> QualityOfService {
        self.subscribe_qos
    }

    pub fn final_qos(&self) -> QualityOfService {
        cmp::min(self.message.qos, self.subscribe_qos)
    }
}

pub trait MessageStore: Send + Sync {
    fn save_received_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: ReceivedPublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn save_pending_message(
        &self,
        client_id: &str,
        message: PendingPublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn retrieve_pending_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<Option<Vec<PendingPublishMessage>>, io::Error>> + Send;

    fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<Option<ReceivedPublishMessage>, io::Error>> + Send;

    fn puback(
        &self,
        client_id: &str,
        server_packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn pubrec(
        &self,
        client_id: &str,
        server_packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn pubcomp(
        &self,
        client_id: &str,
        server_packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn is_full(&self, client_id: &str) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn get_message_count(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<usize, io::Error>> + Send;

    fn clear_all_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<(), io::Error>> + Send;
}
