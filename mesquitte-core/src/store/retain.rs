use std::{future::Future, io, sync::Arc};

use mqtt_codec_kit::common::{QualityOfService, TopicFilter, TopicName};
use mqtt_codec_kit::v5::control::PublishProperties;

use super::message::IncomingPublishMessage;

#[derive(Clone)]
pub struct RetainContent {
    // the publisher client id
    client_id: String,
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

    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }
}

impl<T> From<(T, &IncomingPublishMessage)> for RetainContent
where
    T: Into<String>,
{
    fn from((client_id, packet): (T, &IncomingPublishMessage)) -> Self {
        Self {
            client_id: client_id.into(),
            topic_name: packet.topic_name().clone(),
            payload: packet.payload().into(),
            qos: packet.qos(),
            properties: packet.properties().map(|p| p.to_owned()),
        }
    }
}

pub trait RetainMessageStore {
    fn search(
        &self,
        topic_filter: &TopicFilter,
    ) -> impl Future<Output = Result<Vec<Arc<RetainContent>>, io::Error>> + Send;

    fn insert(
        &self,
        content: RetainContent,
    ) -> impl Future<Output = Result<Option<Arc<RetainContent>>, io::Error>> + Send;

    fn remove(
        &self,
        topic_name: &TopicName,
    ) -> impl Future<Output = Result<Option<Arc<RetainContent>>, io::Error>> + Send;
}
