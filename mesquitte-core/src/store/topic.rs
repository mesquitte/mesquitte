use std::{future::Future, io};

use foldhash::{HashMap, HashMapExt};
use mqtt_codec_kit::common::{QualityOfService, TopicFilter, TopicName};

#[derive(Debug, Clone)]
pub struct TopicContent {
    pub topic_filter: Option<String>,
    pub clients: HashMap<String, QualityOfService>,
    pub shared_clients: HashMap<String, HashMap<String, QualityOfService>>,
}

impl Default for TopicContent {
    fn default() -> Self {
        Self {
            topic_filter: Default::default(),
            clients: HashMap::new(),
            shared_clients: HashMap::new(),
        }
    }
}

pub trait TopicStore: Send + Sync {
    fn match_topic(
        &self,
        topic_name: &TopicName,
    ) -> impl Future<Output = io::Result<Vec<TopicContent>>> + Send;

    fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        qos: QualityOfService,
    ) -> impl Future<Output = io::Result<()>> + Send;

    fn unsubscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
    ) -> impl Future<Output = io::Result<bool>> + Send;
}
