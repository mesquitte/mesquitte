use std::{cmp, fmt::Debug, future::Future, io, sync::Arc};

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

#[derive(Clone, Debug)]
pub struct IncomingPublishMessage {
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,
    retain: bool,
    dup: bool,
    properties: Option<PublishProperties>,
}

impl IncomingPublishMessage {
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

impl From<V4PublishPacket> for IncomingPublishMessage {
    fn from(packet: V4PublishPacket) -> Self {
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

impl From<V5PublishPacket> for IncomingPublishMessage {
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

impl From<Arc<RetainContent>> for IncomingPublishMessage {
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

// #[cfg(feature = "v4")]
impl From<V4LastWill> for IncomingPublishMessage {
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
        }
    }
}

// #[cfg(feature = "v5")]
impl From<V5LastWill> for IncomingPublishMessage {
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
pub struct OutgoingPublishMessage {
    server_packet_id: u16,
    subscribe_qos: QualityOfService,
    message: IncomingPublishMessage,
}

impl OutgoingPublishMessage {
    pub fn new(
        server_packet_id: u16,
        subscribe_qos: QualityOfService,
        message: IncomingPublishMessage,
    ) -> Self {
        Self {
            server_packet_id,
            subscribe_qos,
            message,
        }
    }

    pub fn server_packet_id(&self) -> u16 {
        self.server_packet_id
    }

    pub fn message(&self) -> &IncomingPublishMessage {
        &self.message
    }

    pub fn subscribe_qos(&self) -> QualityOfService {
        self.subscribe_qos
    }

    pub fn final_qos(&self) -> QualityOfService {
        cmp::min(self.message.qos, self.subscribe_qos)
    }
}

pub trait MessageStore {
    fn enqueue_incoming(
        &self,
        client_id: &str,
        packet_id: u16,
        message: IncomingPublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn enqueue_outgoing(
        &self,
        client_id: &str,
        message: OutgoingPublishMessage,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn fetch_pending_outgoing(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<Vec<OutgoingPublishMessage>, io::Error>> + Send;

    fn fetch_ready_incoming(
        &self,
        client_id: &str,
        max_inflight: usize,
    ) -> impl Future<Output = Result<Option<Vec<IncomingPublishMessage>>, io::Error>> + Send;

    fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

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

    fn purge_completed_incoming_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<(), io::Error>> + Send;

    fn purge_completed_outgoing_messages(
        &self,
        client_id: &str,
    ) -> impl Future<Output = Result<(), io::Error>> + Send;

    fn is_full(&self, client_id: &str) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn remove_all(&self, client_id: &str) -> impl Future<Output = Result<(), io::Error>> + Send;
}
