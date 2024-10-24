use std::sync::Arc;

use foldhash::HashSet;
use message::MessageMemoryStore;
use mqtt_codec_kit::common::{TopicFilter, TopicName};
use retain::RetainMessageMemoryStore;
use topic::TopicMemoryStore;

use super::{
    message::{MessageStore, PendingPublishMessage, ReceivedPublishMessage},
    retain::{RetainContent, RetainMessageStore},
    topic::{RouteContent, RouteOption, TopicStore},
};

pub mod message;
pub mod retain;
pub mod topic;

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
    async fn save_received_message(
        &self,
        client_id: &str,
        packet_id: u16,
        message: ReceivedPublishMessage,
    ) -> Result<bool, std::io::Error> {
        self.message_store
            .save_received_message(client_id, packet_id, message)
            .await
    }

    async fn save_pending_message(
        &self,
        client_id: &str,
        server_packet_id: u16,
        message: PendingPublishMessage,
    ) -> Result<bool, std::io::Error> {
        self.message_store
            .save_pending_message(client_id, server_packet_id, message)
            .await
    }

    async fn retrieve_pending_messages(
        &self,
        client_id: &str,
    ) -> Result<Vec<(u16, PendingPublishMessage)>, std::io::Error> {
        self.message_store
            .retrieve_pending_messages(client_id)
            .await
    }

    async fn pubrel(
        &self,
        client_id: &str,
        packet_id: u16,
    ) -> Result<Option<ReceivedPublishMessage>, std::io::Error> {
        self.message_store.pubrel(client_id, packet_id).await
    }

    async fn puback(&self, client_id: &str, server_packet_id: u16) -> Result<bool, std::io::Error> {
        self.message_store.puback(client_id, server_packet_id).await
    }

    async fn pubrec(&self, client_id: &str, server_packet_id: u16) -> Result<bool, std::io::Error> {
        self.message_store.pubrec(client_id, server_packet_id).await
    }

    async fn pubcomp(
        &self,
        client_id: &str,
        server_packet_id: u16,
    ) -> Result<bool, std::io::Error> {
        self.message_store
            .pubcomp(client_id, server_packet_id)
            .await
    }

    async fn clean_received_messages(&self, client_id: &str) -> Result<(), std::io::Error> {
        self.message_store.clean_received_messages(client_id).await
    }

    async fn clean_pending_messages(&self, client_id: &str) -> Result<(), std::io::Error> {
        self.message_store.clean_pending_messages(client_id).await
    }

    async fn is_full(&self, client_id: &str) -> Result<bool, std::io::Error> {
        self.message_store.is_full(client_id).await
    }

    async fn get_message_count(&self, client_id: &str) -> Result<usize, std::io::Error> {
        self.message_store.get_message_count(client_id).await
    }

    async fn clear_all_messages(&self, client_id: &str) -> Result<(), std::io::Error> {
        self.message_store.clear_all_messages(client_id).await
    }
}

impl RetainMessageStore for MemoryStore {
    async fn search(
        &self,
        topic_filter: &TopicFilter,
    ) -> Result<Vec<Arc<RetainContent>>, std::io::Error> {
        self.retain_message_store.search(topic_filter).await
    }

    async fn insert(
        &self,
        content: RetainContent,
    ) -> Result<Option<Arc<RetainContent>>, std::io::Error> {
        self.retain_message_store.insert(content).await
    }

    async fn remove(
        &self,
        topic_name: &TopicName,
    ) -> Result<Option<Arc<RetainContent>>, std::io::Error> {
        self.retain_message_store.remove(topic_name).await
    }
}

impl TopicStore for MemoryStore {
    async fn search(&self, topic_name: &TopicName) -> Result<RouteContent, std::io::Error> {
        self.topic_store.search(topic_name).await
    }

    async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        options: RouteOption,
    ) -> Result<(), std::io::Error> {
        self.topic_store
            .subscribe(client_id, topic_filter, options)
            .await
    }

    async fn unsubscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
    ) -> Result<bool, std::io::Error> {
        self.topic_store.unsubscribe(client_id, topic_filter).await
    }

    async fn unsubscribe_topics(
        &self,
        client_id: &str,
        topics: &HashSet<TopicFilter>,
    ) -> Result<bool, std::io::Error> {
        self.topic_store.unsubscribe_topics(client_id, topics).await
    }
}
