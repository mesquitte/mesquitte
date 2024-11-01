use std::io;

use foldhash::HashMap;
use mqtt_codec_kit::common::{
    QualityOfService, TopicFilter, TopicName, LEVEL_SEP, MATCH_ALL_STR, MATCH_DOLLAR_STR,
    MATCH_ONE_STR,
};
use parking_lot::RwLock;

use crate::store::topic::{TopicContent, TopicStore};

#[derive(Debug, Default)]
pub struct TopicMemoryStore {
    inner: RwLock<TopicNode>,
}

impl TopicStore for TopicMemoryStore {
    async fn match_topic(&self, topic_name: &TopicName) -> io::Result<Vec<TopicContent>> {
        self.inner.match_topic(topic_name).await
    }

    async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        qos: QualityOfService,
    ) -> io::Result<()> {
        self.inner.subscribe(client_id, topic_filter, qos).await
    }

    async fn unsubscribe(&self, client_id: &str, topic_filter: &TopicFilter) -> io::Result<bool> {
        self.inner.unsubscribe(client_id, topic_filter).await
    }
}

#[derive(Debug, Default)]
struct TopicNode {
    children: HashMap<String, Self>,
    topic_content: TopicContent,
}

impl TopicNode {
    fn match_topic(&self, topic_slice: &[&str]) -> Vec<TopicContent> {
        let mut contents = Vec::new();
        if topic_slice.is_empty() {
            contents.push(self.topic_content.clone());
            return contents;
        }

        for key in [topic_slice[0], MATCH_ONE_STR, MATCH_ALL_STR].iter() {
            if let Some(child) = self.children.get(*key) {
                contents.extend(child.match_topic(&topic_slice[1..]));
            }
        }
        contents
    }
}

impl TopicStore for RwLock<TopicNode> {
    async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        qos: QualityOfService,
    ) -> io::Result<()> {
        let (group, levels) = match topic_filter.shared_info() {
            Some((g, t)) => {
                let l: Vec<&str> = t.split(LEVEL_SEP).collect();
                (Some(g), l)
            }
            None => (None, topic_filter.split(LEVEL_SEP).collect()),
        };

        let mut lock = self.write();
        let mut node = &mut *lock;

        let topic_filter = levels.join("/");
        for lv in levels.into_iter() {
            node = node.children.entry(lv.to_string()).or_default();
        }
        node.topic_content.topic_filter = Some(topic_filter);

        match group {
            Some(g) => {
                node.topic_content
                    .shared_clients
                    .entry(g.to_string())
                    .or_default()
                    .insert(client_id.to_string(), qos);
                Ok(())
            }
            None => {
                node.topic_content
                    .clients
                    .insert(client_id.to_string(), qos);
                Ok(())
            }
        }
    }

    async fn unsubscribe(&self, client_id: &str, filter: &TopicFilter) -> io::Result<bool> {
        let mut lock = self.write();
        let mut node = &mut *lock;
        let (group, levels) = match filter.shared_info() {
            Some((group, topic)) => (Some(group), topic.split(LEVEL_SEP)),
            None => (None, filter.split(LEVEL_SEP)),
        };

        for lv in levels {
            if let Some(child) = node.children.get_mut(lv) {
                node = child;
            } else {
                return Ok(false);
            }
        }
        match group {
            Some(group) => {
                if let Some(shared_clients) = node.topic_content.shared_clients.get_mut(group) {
                    shared_clients.retain(|k, _| k != client_id);
                    if shared_clients.is_empty() {
                        node.topic_content.shared_clients.remove(group);
                    }
                }
            }
            None => node.topic_content.clients.retain(|k, _| k != client_id),
        };
        Ok(true)
    }

    async fn match_topic(&self, topic_name: &TopicName) -> io::Result<Vec<TopicContent>> {
        if topic_name.starts_with(MATCH_DOLLAR_STR) {
            return Ok(Vec::new());
        }

        let lock = self.read();
        let topic_levels: Vec<&str> = topic_name.split(LEVEL_SEP).collect();
        Ok(lock.match_topic(&topic_levels))
    }
}
