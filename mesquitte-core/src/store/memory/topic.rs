use std::{io, sync::Arc};

use foldhash::{HashMap, HashMapExt, HashSet};
use mqtt_codec_kit::common::{TopicFilter, TopicName, LEVEL_SEP, MATCH_ALL_STR, MATCH_ONE_STR};
use parking_lot::RwLock;

use crate::store::topic::{RouteContent, RouteOption, TopicStore};

#[derive(Debug, Default)]
struct TrieNode {
    children: HashMap<String, Arc<RwLock<TrieNode>>>,
    clients: HashMap<String, RouteOption>,
    shared_clients: HashMap<String, HashMap<String, RouteOption>>,
}

#[derive(Default)]
pub struct TopicMemoryStore {
    root: Arc<RwLock<TrieNode>>,
}

impl TopicMemoryStore {
    fn match_topic(
        node: Arc<RwLock<TrieNode>>,
        topic_levels: &[&str],
        current_level: usize,
        result: &mut RouteContent,
    ) {
        let node_read = node.read();

        if current_level == topic_levels.len() {
            result.normal_clients.extend(
                node_read
                    .clients
                    .iter()
                    .map(|(id, option)| (id.clone(), option.clone())),
            );
            for (group, clients) in &node_read.shared_clients {
                result
                    .shared_clients
                    .entry(group.clone())
                    .or_default()
                    .extend(
                        clients
                            .iter()
                            .map(|(id, option)| (id.clone(), option.clone())),
                    );
            }
            return;
        }

        let level = topic_levels[current_level];

        if let Some(next_node) = node_read.children.get(level) {
            Self::match_topic(next_node.clone(), topic_levels, current_level + 1, result);
        }

        if let Some(plus_node) = node_read.children.get(MATCH_ONE_STR) {
            Self::match_topic(plus_node.clone(), topic_levels, current_level + 1, result);
        }

        if let Some(hash_node) = node_read.children.get(MATCH_ALL_STR) {
            result.normal_clients.extend(
                hash_node
                    .read()
                    .clients
                    .iter()
                    .map(|(id, option)| (id.clone(), option.clone())),
            );

            for (group, clients) in &hash_node.read().shared_clients {
                result
                    .shared_clients
                    .entry(group.clone())
                    .or_default()
                    .extend(
                        clients
                            .iter()
                            .map(|(id, option)| (id.clone(), option.clone())),
                    );
            }
        }
    }
}

impl TopicStore for TopicMemoryStore {
    async fn search(&self, topic_name: &TopicName) -> Result<RouteContent, io::Error> {
        let topic_levels: Vec<&str> = topic_name.split('/').collect();
        let mut result = RouteContent {
            normal_clients: Vec::new(),
            shared_clients: HashMap::new(),
        };
        Self::match_topic(self.root.clone(), &topic_levels, 0, &mut result);
        Ok(result)
    }

    async fn subscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
        options: RouteOption,
    ) -> Result<(), io::Error> {
        let (group, levels) = match topic_filter.shared_info() {
            Some((group, topic)) => (Some(group), topic.split(LEVEL_SEP)),
            None => (None, topic_filter.split(LEVEL_SEP)),
        };

        let mut current_node = self.root.clone();
        for level in levels {
            let next_node = {
                let mut node = current_node.write();
                node.children
                    .entry(level.to_string())
                    .or_insert_with(|| Arc::new(RwLock::new(TrieNode::default())))
                    .clone()
            };
            current_node = next_node;
        }

        match group {
            Some(group) => current_node
                .write()
                .shared_clients
                .entry(group.to_string())
                .or_default()
                .insert(client_id.to_string(), options),
            None => current_node
                .write()
                .clients
                .insert(client_id.to_string(), options),
        };
        Ok(())
    }

    async fn unsubscribe(
        &self,
        client_id: &str,
        topic_filter: &TopicFilter,
    ) -> Result<bool, io::Error> {
        let (group, levels) = match topic_filter.shared_info() {
            Some((group, topic)) => (Some(group), topic.split(LEVEL_SEP)),
            None => (None, topic_filter.split(LEVEL_SEP)),
        };

        let mut current_node = self.root.clone();
        let mut need_clean_nodes = Vec::new();
        for level in levels {
            let next_node = {
                let node = current_node.read();
                if let Some(child) = node.children.get(level) {
                    need_clean_nodes.push((current_node.clone(), level.to_string()));
                    child.clone()
                } else {
                    return Ok(false);
                }
            };
            current_node = next_node;
        }

        match group {
            Some(group) => match current_node.write().shared_clients.get_mut(group) {
                Some(clients) => {
                    if clients.remove(client_id).is_none() {
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            },
            None => {
                if current_node.write().clients.remove(client_id).is_none() {
                    return Ok(false);
                }
            }
        };

        for (node, level) in need_clean_nodes.into_iter().rev() {
            let remove_child = {
                let node_read = node.read();
                if let Some(child) = node_read.children.get(&level) {
                    let child_read = child.read();
                    child_read.children.is_empty()
                        && child_read.clients.is_empty()
                        && child_read.shared_clients.is_empty()
                } else {
                    false
                }
            };

            if remove_child {
                node.write().children.remove(&level);
            }
        }

        Ok(true)
    }

    async fn unsubscribe_topics(
        &self,
        client_id: &str,
        topics: &HashSet<TopicFilter>,
    ) -> Result<bool, io::Error> {
        let mut need_clean_nodes = Vec::new();
        for topic_filter in topics {
            let (group, levels) = match topic_filter.shared_info() {
                Some((group, topic)) => (Some(group), topic.split(LEVEL_SEP)),
                None => (None, topic_filter.split(LEVEL_SEP)),
            };

            let mut current_node = self.root.clone();
            for level in levels {
                let next_node = {
                    let node = current_node.read();
                    if let Some(child) = node.children.get(level) {
                        need_clean_nodes.push((current_node.clone(), level.to_string()));
                        child.clone()
                    } else {
                        return Ok(false);
                    }
                };
                current_node = next_node;
            }

            match group {
                Some(group) => match current_node.write().shared_clients.get_mut(group) {
                    Some(clients) => {
                        if clients.remove(client_id).is_none() {
                            return Ok(false);
                        }
                    }
                    None => {
                        return Ok(false);
                    }
                },
                None => {
                    if current_node.write().clients.remove(client_id).is_none() {
                        return Ok(false);
                    }
                }
            };
        }

        for (node, level) in need_clean_nodes.into_iter().rev() {
            let remove_child = {
                let node_read = node.read();
                if let Some(child) = node_read.children.get(&level) {
                    let child_read = child.read();
                    child_read.children.is_empty()
                        && child_read.clients.is_empty()
                        && child_read.shared_clients.is_empty()
                } else {
                    false
                }
            };

            if remove_child {
                node.write().children.remove(&level);
            }
        }

        Ok(true)
    }
}
