use std::io;
use std::{fmt::Debug, future::Future};

use foldhash::{HashMap, HashSet};
use mqtt_codec_kit::common::{QualityOfService, TopicFilter, TopicName};
use mqtt_codec_kit::v5::packet::subscribe::SubscribeOptions;

#[derive(Debug, Clone)]
pub enum RouteOption {
    V4(QualityOfService),
    V5(SubscribeOptions),
}

#[derive(Debug, Clone)]
pub struct RouteContent {
    pub normal_clients: Vec<(String, RouteOption)>,
    pub shared_clients: HashMap<String, Vec<(String, RouteOption)>>,
}

pub trait TopicStore: Send + Sync {
    fn search(
        &self,
        topic_name: &TopicName,
    ) -> impl Future<Output = Result<RouteContent, io::Error>> + Send;

    fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        options: RouteOption,
    ) -> impl Future<Output = Result<(), io::Error>> + Send;

    fn unsubscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;

    fn unsubscribe_topics(
        &self,
        client_id: &str,
        topics: &HashSet<TopicFilter>,
    ) -> impl Future<Output = Result<bool, io::Error>> + Send;
}
