use message::MessageStore;
use retain::RetainMessageStore;
use topic::TopicStore;

pub mod memory;
pub mod message;
pub mod retain;
pub mod topic;

pub struct Storage<MS, RS, TS>
where
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    message_store: MS,
    retain_message_store: RS,
    topic_store: TS,
}

impl<MS, RS, TS> Storage<MS, RS, TS>
where
    MS: MessageStore,
    RS: RetainMessageStore,
    TS: TopicStore,
{
    pub fn new(message_store: MS, retain_message_store: RS, topic_store: TS) -> Self {
        Self {
            topic_store,
            message_store,
            retain_message_store,
        }
    }

    pub fn topic_store(&self) -> &TS {
        &self.topic_store
    }

    pub fn message_store(&self) -> &MS {
        &self.message_store
    }

    pub fn retain_message_store(&self) -> &RS {
        &self.retain_message_store
    }
}
