use std::sync::Arc;

use message::MessageMemoryStore;
use mqtt_codec_kit::common::{QualityOfService, TopicFilter, TopicName};
use retain::RetainMessageMemoryStore;
use topic::TopicMemoryStore;

use super::{
    message::{MessageStore, PendingPublishMessage, PublishMessage},
    retain::RetainMessageStore,
    topic::{TopicContent, TopicStore},
};

pub mod message;
pub mod retain;
pub mod topic;

#[derive(Default)]
pub struct MemoryStore {
    message_store: MessageMemoryStore,
    retain_message_store: RetainMessageMemoryStore,
    topic_store: TopicMemoryStore,
}

impl MemoryStore {
    pub fn new(
        message_store: MessageMemoryStore,
        retain_message_store: RetainMessageMemoryStore,
        topic_store: TopicMemoryStore,
    ) -> Self {
        Self {
            message_store,
            retain_message_store,
            topic_store,
        }
    }
}

impl MessageStore for MemoryStore {
    async fn save_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PublishMessage,
    ) -> Result<bool, std::io::Error> {
        self.message_store
            .save_publish_message(client_id, packet_id, message)
            .await
    }

    async fn save_pending_publish_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: PendingPublishMessage,
    ) -> Result<bool, std::io::Error> {
        self.message_store
            .save_pending_publish_message(client_id, packet_id, message)
            .await
    }

    async fn try_get_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Option<Vec<(u16, PendingPublishMessage)>>, std::io::Error> {
        self.message_store.try_get_pending_messages(client_id).await
    }

    async fn get_all_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Option<Vec<(u16, PendingPublishMessage)>>, std::io::Error> {
        self.message_store.get_all_pending_messages(client_id).await
    }

    async fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<PublishMessage>, std::io::Error> {
        self.message_store.pubrel(client_id, packet_id).await
    }

    async fn puback(&self, client_id: &str, packet_id: u16) -> Result<bool, std::io::Error> {
        self.message_store.puback(client_id, packet_id).await
    }

    async fn pubrec(&self, client_id: &str, packet_id: u16) -> Result<bool, std::io::Error> {
        self.message_store.pubrec(client_id, packet_id).await
    }

    async fn pubcomp(&self, client_id: &str, packet_id: u16) -> Result<bool, std::io::Error> {
        self.message_store.pubcomp(client_id, packet_id).await
    }

    async fn is_full(&self, client_id: &str) -> Result<bool, std::io::Error> {
        self.message_store.is_full(client_id).await
    }

    async fn message_count(&self, client_id: &str) -> Result<usize, std::io::Error> {
        self.message_store.message_count(client_id).await
    }

    async fn clear_all(&self, client_id: &str) -> Result<(), std::io::Error> {
        self.message_store.clear_all(client_id).await
    }
}

impl RetainMessageStore for MemoryStore {
    async fn search(
        &self,
        topic_filter: &TopicFilter,
    ) -> Result<Vec<Arc<PublishMessage>>, std::io::Error> {
        self.retain_message_store.search(topic_filter).await
    }

    async fn insert(
        &self,
        content: PublishMessage,
    ) -> Result<Option<Arc<PublishMessage>>, std::io::Error> {
        self.retain_message_store.insert(content).await
    }

    async fn remove(
        &self,
        topic_name: &TopicName,
    ) -> Result<Option<Arc<PublishMessage>>, std::io::Error> {
        self.retain_message_store.remove(topic_name).await
    }
}

impl TopicStore for MemoryStore {
    async fn match_topic(
        &self,
        topic_name: &TopicName,
    ) -> Result<Vec<TopicContent>, std::io::Error> {
        self.topic_store.match_topic(topic_name).await
    }

    async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        qos: QualityOfService,
    ) -> Result<(), std::io::Error> {
        self.topic_store
            .subscribe(client_id, topic_filter, qos)
            .await
    }

    async fn unsubscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
    ) -> Result<bool, std::io::Error> {
        self.topic_store.unsubscribe(client_id, topic_filter).await
    }
}
