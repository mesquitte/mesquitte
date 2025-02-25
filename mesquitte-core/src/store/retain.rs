use std::{future::Future, io, sync::Arc};

use mqtt_codec_kit::common::{TopicFilter, TopicName};

use super::message::PublishMessage;

pub trait RetainMessageStore: Send + Sync {
    fn search(
        &self,
        topic_filter: &TopicFilter,
    ) -> impl Future<Output = Result<Vec<Arc<PublishMessage>>, io::Error>> + Send;

    fn insert(
        &self,
        content: PublishMessage,
    ) -> impl Future<Output = Result<Option<Arc<PublishMessage>>, io::Error>> + Send;

    fn remove(
        &self,
        topic_name: &TopicName,
    ) -> impl Future<Output = Result<Option<Arc<PublishMessage>>, io::Error>> + Send;
}
