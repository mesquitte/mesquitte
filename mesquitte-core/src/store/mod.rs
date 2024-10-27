use std::ops::Deref;

use message::MessageStore;
use retain::RetainMessageStore;
use topic::TopicStore;

pub mod memory;
pub mod message;
pub mod retain;
pub mod topic;

pub struct Storage<S>(S);

impl<S> Storage<S>
where
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(inner: S) -> Self {
        Self(inner)
    }
}

impl<S> AsRef<S> for Storage<S> {
    fn as_ref(&self) -> &S {
        self.deref()
    }
}

impl<S> Deref for Storage<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
