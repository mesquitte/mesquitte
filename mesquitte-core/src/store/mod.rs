use message::MessageStore;
use retain::RetainMessageStore;
use topic::TopicStore;

pub mod memory;
pub mod message;
pub mod retain;
pub mod topic;

pub struct Storage<S> {
    pub inner: S,
}

impl<S> Storage<S>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}
