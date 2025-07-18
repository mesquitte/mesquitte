use std::{io, sync::Arc};

use foldhash::HashMap;
use mqtt_codec_kit::common::{
    LEVEL_SEP, MATCH_ALL_CHAR, MATCH_ALL_STR, MATCH_ONE_CHAR, MATCH_ONE_STR, TopicName,
};
use parking_lot::RwLock;

use crate::store::{message::PublishMessage, retain::RetainMessageStore};

fn split_topic(topic: &str) -> (&str, Option<&str>) {
    if let Some((head, rest)) = topic.split_once(LEVEL_SEP) {
        (head, Some(rest))
    } else {
        (topic, None)
    }
}

#[derive(Default)]
struct RetainNode {
    content: Option<Arc<PublishMessage>>,
    nodes: RwLock<HashMap<String, RetainNode>>,
}

#[derive(Default)]
pub struct RetainMessageMemoryStore {
    inner: RetainNode,
}

impl RetainNode {
    fn is_empty(&self) -> bool {
        self.content.is_none() && self.nodes.read().is_empty()
    }

    fn get_matches(
        &self,
        prev_item: &str,
        filter_items: Option<&str>,
        wildcard_first: bool,
        retains: &mut Vec<Arc<PublishMessage>>,
    ) {
        match prev_item {
            MATCH_ALL_STR => {
                assert!(filter_items.is_none(), "invalid topic filter");
                let nodes = self.nodes.read();
                for node in nodes.values() {
                    node.get_matches(MATCH_ALL_STR, None, wildcard_first, retains);
                }
                // Topic name "abc" will match topic filter "abc/#", since "#" also represent parent level.
                if let Some(content) = self.content.as_ref() {
                    if !(content.topic_name().starts_with('$') && wildcard_first) {
                        retains.push(Arc::clone(content));
                    }
                }
            }
            MATCH_ONE_STR => {
                let nodes = self.nodes.read();
                if let Some((filter_item, rest_items)) = filter_items.map(split_topic) {
                    for node in nodes.values() {
                        node.get_matches(filter_item, rest_items, wildcard_first, retains);
                    }
                } else {
                    for node in nodes.values() {
                        if let Some(content) = node.content.as_ref() {
                            if !(content.topic_name().starts_with('$') && wildcard_first) {
                                retains.push(Arc::clone(content));
                            }
                        }
                    }
                }
            }
            _ => {
                let nodes = self.nodes.read();
                if let Some(node) = nodes.get(prev_item) {
                    if let Some((filter_item, rest_items)) = filter_items.map(split_topic) {
                        node.get_matches(filter_item, rest_items, wildcard_first, retains);
                    } else if let Some(content) = node.content.as_ref() {
                        if !(content.topic_name().starts_with('$') && wildcard_first) {
                            retains.push(Arc::clone(content));
                        }
                    }
                }
            }
        }
    }

    fn insert(
        &self,
        prev_item: &str,
        topic_items: Option<&str>,
        content: Arc<PublishMessage>,
    ) -> Option<Arc<PublishMessage>> {
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(prev_item) {
            if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                node.insert(topic_item, rest_items, content)
            } else {
                node.content.replace(content)
            }
        } else {
            let mut new_node = RetainNode::default();
            if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                new_node.insert(topic_item, rest_items, content);
            } else {
                new_node.content = Some(content);
            }
            nodes.insert(prev_item.to_string(), new_node);
            None
        }
    }

    fn remove(&self, prev_item: &str, topic_items: Option<&str>) -> Option<Arc<PublishMessage>> {
        let mut old_content = None;
        let mut remove_node = false;
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(prev_item) {
            old_content = if let Some((topic_item, rest_items)) = topic_items.map(split_topic) {
                node.remove(topic_item, rest_items)
            } else {
                node.content.take()
            };
            remove_node = node.is_empty();
        }
        if remove_node {
            nodes.remove(prev_item);
        }
        old_content
    }
}

impl RetainMessageStore for RetainMessageMemoryStore {
    async fn search(
        &self,
        topic_filter: &mqtt_codec_kit::common::TopicFilter,
    ) -> Result<Vec<Arc<PublishMessage>>, io::Error> {
        // [MQTT-4.7.2-1] The Server MUST NOT match Topic Filters starting with a
        // wildcard character (# or +) with Topic Names beginning with a $ character
        let wildcard_first = topic_filter.starts_with([MATCH_ONE_CHAR, MATCH_ALL_CHAR]);

        let (filter_item, rest_items) = split_topic(topic_filter);
        let mut retains = Vec::new();
        self.inner
            .get_matches(filter_item, rest_items, wildcard_first, &mut retains);

        Ok(retains)
    }

    async fn insert(
        &self,
        content: PublishMessage,
    ) -> Result<Option<Arc<PublishMessage>>, io::Error> {
        let topic_name = content.topic_name().to_owned();
        let (topic_item, rest_items) = split_topic(&topic_name);

        Ok(self.inner.insert(topic_item, rest_items, Arc::new(content)))
    }

    async fn remove(
        &self,
        topic_name: &TopicName,
    ) -> Result<Option<Arc<PublishMessage>>, io::Error> {
        let (topic_item, rest_items) = split_topic(topic_name);
        Ok(self.inner.remove(topic_item, rest_items))
    }
}
