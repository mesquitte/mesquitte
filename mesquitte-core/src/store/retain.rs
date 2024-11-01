use std::{future::Future, io, sync::Arc};

use mqtt_codec_kit::common::{QualityOfService, TopicFilter, TopicName};
#[cfg(feature = "v5")]
use mqtt_codec_kit::v5::control::PublishProperties;

use super::message::PublishMessage;

#[derive(Clone)]
pub struct RetainContent {
    // the publisher client id
    client_id: String,
    topic_name: TopicName,
    payload: Vec<u8>,
    qos: QualityOfService,
    #[cfg(feature = "v5")]
    properties: Option<PublishProperties>,
}

impl RetainContent {
    pub fn topic_name(&self) -> &TopicName {
        &self.topic_name
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_ref()
    }

    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    #[cfg(feature = "v5")]
    pub fn properties(&self) -> Option<&PublishProperties> {
        self.properties.as_ref()
    }
}

impl<T> From<(T, &PublishMessage)> for RetainContent
where
    T: Into<String>,
{
    fn from((client_id, packet): (T, &PublishMessage)) -> Self {
        Self {
            client_id: client_id.into(),
            topic_name: packet.topic_name().clone(),
            payload: packet.payload().into(),
            qos: packet.qos(),
            #[cfg(feature = "v5")]
            properties: packet.properties().map(|p| p.to_owned()),
        }
    }
}

pub trait RetainMessageStore: Send + Sync {
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
