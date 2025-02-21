use std::{io, sync::Arc};

use foldhash::HashMap;
use mqtt_codec_kit::common::{
    LEVEL_SEP, MATCH_ALL_STR, MATCH_DOLLAR_STR, MATCH_ONE_STR, QualityOfService, TopicFilter,
    TopicName,
};
use parking_lot::RwLock;

use crate::store::topic::{TopicContent, TopicStore};

#[derive(Debug, Default)]
pub struct TopicMemoryStore {
    root: Arc<RwLock<TopicNode>>,
}

impl TopicStore for TopicMemoryStore {
    async fn match_topic(&self, topic_name: &TopicName) -> io::Result<Vec<TopicContent>> {
        if topic_name.starts_with(MATCH_DOLLAR_STR) {
            return Ok(Vec::new());
        }

        let topic_levels: Vec<&str> = topic_name.split(LEVEL_SEP).collect();
        let contents = self.root.read().match_topic(&topic_levels);
        Ok(contents)
    }

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

        let mut current_node = self.root.clone();
        for lv in &levels {
            let temp = {
                current_node
                    .write()
                    .children
                    .entry(lv.to_string())
                    .or_default()
                    .clone()
            };
            current_node = temp;
        }

        let mut node_write_guard = current_node.write();
        node_write_guard.topic_content.topic_filter = Some(levels.join("/"));
        let clients = match group {
            Some(g) => node_write_guard
                .topic_content
                .shared_clients
                .entry(g.to_string())
                .or_default(),
            None => &mut node_write_guard.topic_content.clients,
        };
        clients.insert(client_id.to_string(), qos);
        Ok(())
    }

    async fn unsubscribe(&self, client_id: &str, topic_filter: &TopicFilter) -> io::Result<bool> {
        let (group, levels) = match topic_filter.shared_info() {
            Some((group, topic)) => (Some(group), topic.split(LEVEL_SEP)),
            None => (None, topic_filter.split(LEVEL_SEP)),
        };

        let mut current_node = self.root.clone();
        let mut parent_stack = Vec::new();
        for lv in levels {
            let temp = {
                if let Some(child) = current_node.read().children.get(lv) {
                    child.clone()
                } else {
                    return Ok(false);
                }
            };
            parent_stack.push((current_node.clone(), lv.to_string()));
            current_node = temp;
        }

        match group {
            Some(group) => {
                if let Some(shared_clients) = current_node
                    .write()
                    .topic_content
                    .shared_clients
                    .get_mut(group)
                {
                    shared_clients.retain(|k, _| k != client_id);
                    if shared_clients.is_empty() {
                        shared_clients.remove(group);
                    }
                }
            }
            None => current_node
                .write()
                .topic_content
                .clients
                .retain(|k, _| k != client_id),
        };

        let need_cleanup = {
            let c = current_node.read();
            c.children.is_empty() && c.topic_content.is_empty()
        };
        if need_cleanup {
            while let Some((parent, level)) = parent_stack.pop() {
                let mut parent_write_guard = parent.write();
                parent_write_guard.children.remove(&level);

                if !parent_write_guard.children.is_empty()
                    || !parent_write_guard.topic_content.clients.is_empty()
                    || !parent_write_guard.topic_content.shared_clients.is_empty()
                {
                    break;
                }
            }
        }
        Ok(true)
    }
}

#[derive(Debug, Default)]
struct TopicNode {
    children: HashMap<String, Arc<RwLock<TopicNode>>>,
    topic_content: TopicContent,
}

impl TopicContent {
    fn is_empty(&self) -> bool {
        self.clients.is_empty() && self.shared_clients.is_empty()
    }
}

impl TopicNode {
    fn match_topic(&self, topic_slice: &[&str]) -> Vec<TopicContent> {
        let mut contents = Vec::new();
        if topic_slice.is_empty() {
            if !self.topic_content.is_empty() {
                contents.push(self.topic_content.clone());
            }

            if let Some(child) = self.children.get(MATCH_ALL_STR) {
                contents.extend(child.read().collect_all_contents());
            }
            return contents;
        }

        // Exact match
        if let Some(child) = self.children.get(topic_slice[0]) {
            contents.extend(child.read().match_topic(&topic_slice[1..]));
        }

        // Single-level wildcard '+'
        if let Some(child) = self.children.get(MATCH_ONE_STR) {
            contents.extend(child.read().match_topic(&topic_slice[1..]));
        }

        if let Some(child) = self.children.get(MATCH_ALL_STR) {
            contents.extend(child.read().collect_all_contents());
        }
        contents
    }

    fn collect_all_contents(&self) -> Vec<TopicContent> {
        let mut contents = Vec::new();
        contents.push(self.topic_content.clone());
        for child in self.children.values() {
            contents.extend(child.read().collect_all_contents());
        }
        contents
    }
}
