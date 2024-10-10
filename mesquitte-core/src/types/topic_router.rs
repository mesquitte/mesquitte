use std::{hash::Hash, ops::Deref, sync::Arc};

use ahash::RandomState;
use hashbrown::HashMap;
use mqtt_codec_kit::common::{
    QualityOfService, TopicFilter, TopicName, MATCH_ALL_STR, MATCH_ONE_STR,
};
use parking_lot::RwLock;

use super::retain_table::split_topic;

#[derive(Default)]
pub struct RouteTable {
    nodes: RwLock<HashMap<String, RouteNode>>,
}

struct RouteNode {
    content: Arc<RwLock<RouteContent>>,
    nodes: Arc<RwLock<HashMap<String, RouteNode>>>,
}

#[derive(Debug, Clone)]
pub struct RouteContent {
    /// Returned RouteContent always have topic_filter
    pub topic_filter: Option<TopicFilter>,
    pub clients: HashMap<String, QualityOfService>,
    pub groups: HashMap<String, SharedClients>,
}

#[derive(Debug, Clone, Default)]
pub struct SharedClients {
    hash_builder: RandomState,
    items: Vec<(String, QualityOfService)>,
    index: HashMap<String, usize>,
}

impl RouteTable {
    pub fn get_matches(&self, topic_name: &TopicName) -> Vec<Arc<RwLock<RouteContent>>> {
        let (topic_item, rest_items) = split_topic(topic_name);
        let mut filters = Vec::new();

        let nodes = self.nodes.read();
        if let Some(node) = nodes.get(topic_item) {
            node.get_matches(topic_item, rest_items, &mut filters);
        }
        // [MQTT-4.7.2-1] The Server MUST NOT match Topic Filters starting with a
        // wildcard character (# or +) with Topic Names beginning with a $ character
        if !topic_name.starts_with('$') {
            for item in [MATCH_ALL_STR, MATCH_ONE_STR] {
                if let Some(node) = nodes.get(item) {
                    node.get_matches(item, rest_items, &mut filters);
                }
            }
        }
        filters
    }

    pub fn subscribe(&self, topic_filter: &TopicFilter, id: &str, qos: QualityOfService) {
        if let Some((shared_group_name, shared_filter)) = topic_filter.shared_info() {
            self.subscribe_shared(
                &TopicFilter::new(shared_filter.to_owned()).expect("shared filter"),
                id,
                qos,
                Some(shared_group_name.to_owned()),
            );
        } else {
            self.subscribe_shared(topic_filter, id, qos, None);
        }
    }

    fn subscribe_shared(
        &self,
        topic_filter: &TopicFilter,
        id: &str,
        qos: QualityOfService,
        group: Option<String>,
    ) {
        let (filter_item, rest_items) = split_topic(topic_filter);
        // Since subscribe is not an frequent action, string clone here is acceptable.
        self.nodes
            .write()
            .entry(filter_item.to_string())
            .or_insert_with(RouteNode::new)
            .insert(topic_filter, rest_items, id, qos, group);
    }

    pub fn unsubscribe(&self, topic_filter: &TopicFilter, id: &str) {
        if let Some((shared_group_name, shared_filter)) = topic_filter.shared_info() {
            self.unsubscribe_shared(
                &TopicFilter::new(shared_filter.to_owned()).expect("new topic filter"),
                id,
                Some(shared_group_name),
            );
        } else {
            self.unsubscribe_shared(topic_filter, id, None);
        }
    }

    fn unsubscribe_shared(&self, topic_filter: &TopicFilter, id: &str, group: Option<&str>) {
        let (filter_item, rest_items) = split_topic(topic_filter.deref());
        // bool variable is for resolve dead lock of access `self.nodes`
        let mut remove_node = false;
        let mut nodes = self.nodes.write();
        if let Some(node) = nodes.get_mut(filter_item) {
            remove_node = node.remove(rest_items, id, group);
        }
        if remove_node {
            nodes.remove(filter_item);
        }
    }
}

impl RouteNode {
    fn new() -> RouteNode {
        RouteNode {
            content: Arc::new(RwLock::new(RouteContent {
                topic_filter: None,
                clients: HashMap::new(),
                groups: HashMap::new(),
            })),
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_matches(
        &self,
        prev_item: &str,
        topic_items: Option<&str>,
        filters: &mut Vec<Arc<RwLock<RouteContent>>>,
    ) {
        if prev_item == MATCH_ALL_STR {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }
        } else if let Some(topic_items) = topic_items {
            let nodes = self.nodes.read();
            let (topic_item, rest_items) = split_topic(topic_items);
            for item in [topic_item, MATCH_ALL_STR, MATCH_ONE_STR] {
                if let Some(node) = nodes.get(item) {
                    node.get_matches(item, rest_items, filters);
                }
            }
        } else {
            if !self.content.read().is_empty() {
                filters.push(Arc::clone(&self.content));
            }

            // Topic name "abc" will match topic filter "abc/#", since "#" also represent parent level.
            // NOTE: [locks]
            //   * nodes read
            //   * content read
            if let Some(node) = self.nodes.read().get(MATCH_ALL_STR) {
                if !node.content.read().is_empty() {
                    filters.push(Arc::clone(&node.content));
                }
            }
        }
    }

    fn insert(
        &self,
        topic_filter: &TopicFilter,
        filter_items: Option<&str>,
        id: &str,
        qos: QualityOfService,
        group: Option<String>,
    ) {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            self.nodes
                .write()
                .entry(filter_item.to_string())
                .or_insert_with(RouteNode::new)
                .insert(topic_filter, rest_items, id, qos, group);
        } else {
            let mut content = self.content.write();
            if content.topic_filter.is_none() {
                content.topic_filter = Some(topic_filter.clone());
            }
            if let Some(name) = group {
                content
                    .groups
                    .entry(name)
                    .or_insert_with(SharedClients::default)
                    .insert((id.to_owned(), qos));
            } else {
                content.clients.insert(id.to_owned(), qos);
            }
        }
    }

    fn remove(&self, filter_items: Option<&str>, id: &str, group: Option<&str>) -> bool {
        if let Some(filter_items) = filter_items {
            let (filter_item, rest_items) = split_topic(filter_items);
            // bool variables are for resolve dead lock of access `self.nodes`

            // NOTE: [locks]
            //   * nodes write
            //   * content read
            let mut nodes = self.nodes.write();
            let mut remove_node = false;
            if let Some(node) = nodes.get_mut(filter_item) {
                if node.remove(rest_items, id, group) {
                    remove_node = true;
                }
            }
            let remove_parent = if remove_node {
                nodes.remove(filter_item);
                // NOTE: careful lock order
                self.content.read().is_empty() && nodes.is_empty()
            } else {
                false
            };
            return remove_parent;
        } else {
            let mut content = self.content.write();
            if let Some(name) = group {
                if let Some(shared_clients) = content.groups.get_mut(name) {
                    shared_clients.remove(id);
                    if shared_clients.is_empty() {
                        content.groups.remove(name);
                    }
                }
            } else {
                content.clients.remove(id);
            }
            if content.is_empty() {
                content.topic_filter = None;
                if self.nodes.read().is_empty() {
                    return true;
                }
            }
        }
        false
    }
}

impl RouteContent {
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty() && self.groups.is_empty()
    }
}

impl SharedClients {
    pub fn get_by_hash<T: Hash>(&self, data: T) -> (String, QualityOfService) {
        let number = self.hash_builder.hash_one(data);
        self.get_by_number(number)
    }

    pub fn get_by_number(&self, number: u64) -> (String, QualityOfService) {
        // Empty SharedClients MUST already removed from parent data structure immediately.
        debug_assert!(!self.items.is_empty());
        let idx = number as usize % self.items.len();
        let (client_id, qos) = &self.items[idx];
        (client_id.to_owned(), qos.to_owned())
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    fn insert(&mut self, item: (String, QualityOfService)) {
        if let Some(idx) = self.index.get(&item.0) {
            self.items[*idx].1 = item.1;
        } else {
            self.index.insert(item.0.to_owned(), self.items.len());
            self.items.push(item);
        }
    }

    fn remove(&mut self, item_key: &str) {
        if let Some(idx) = self.index.remove(item_key) {
            if self.items.len() == 1 {
                self.items.clear();
            } else {
                let last_client_id = self.items.last().expect("shared items").0.to_owned();
                self.items.swap_remove(idx);
                self.index.insert(last_client_id, idx);
                if self.items.capacity() >= 16 && self.items.capacity() >= (self.items.len() << 2) {
                    self.items.shrink_to(self.items.len() << 1);
                }
            }
        }
    }
}
