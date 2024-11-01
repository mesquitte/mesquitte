use std::{fmt, mem};

use foldhash::{HashSet, HashSetExt};
use mqtt_codec_kit::{common::TopicFilter, v4::packet::connect::LastWill};
use tokio::time::Instant;

#[derive(Clone)]
pub struct Session {
    connected_at: Instant,
    // last package timestamp
    last_packet_at: Instant,
    // For record packet id send from server to client
    server_packet_id: u16,

    client_id: String,
    username: Option<String>,
    keep_alive: u16,
    clean_session: bool,
    last_will: Option<LastWill>,
    subscriptions: HashSet<TopicFilter>,

    assigned_client_id: bool,
    client_disconnected: bool,
    server_disconnected: bool,
}

impl Session {
    pub fn new(client_id: &str, assigned_client_id: bool) -> Self {
        Self {
            connected_at: Instant::now(),
            last_packet_at: Instant::now(),
            server_packet_id: 1,

            client_id: client_id.to_string(),
            assigned_client_id,
            username: None,
            keep_alive: 0,
            clean_session: true,
            last_will: None,
            subscriptions: HashSet::new(),

            client_disconnected: false,
            server_disconnected: false,
        }
    }

    pub fn last_packet_at(&self) -> &Instant {
        &self.last_packet_at
    }

    pub fn renew_last_packet_at(&mut self) {
        self.last_packet_at = Instant::now();
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn set_username(&mut self, username: Option<String>) {
        self.username = username
    }

    pub fn keep_alive(&self) -> u16 {
        self.keep_alive
    }

    pub fn set_keep_alive(&mut self, keep_alive: u16) {
        // TODO: config: max keep alive?
        // TODO: config: min keep alive?
        // let keep_alive = if keep_alive > self.config.max_keep_alive {
        //     self.server_keep_alive = true;
        //     self.config.max_keep_alive
        // } else if keep_alive < self.config.min_keep_alive {
        //     self.server_keep_alive = true;
        //     self.config.min_keep_alive
        // } else {
        //     keep_alive
        // };

        self.keep_alive = keep_alive;
    }

    pub fn clean_session(&self) -> bool {
        self.clean_session
    }

    pub fn disconnected(&self) -> bool {
        self.client_disconnected || self.server_disconnected
    }

    pub fn client_disconnected(&self) -> bool {
        self.client_disconnected
    }

    pub fn set_client_disconnected(&mut self) {
        self.client_disconnected = true
    }

    pub fn server_disconnected(&self) -> bool {
        self.server_disconnected
    }

    pub fn set_server_disconnected(&mut self) {
        self.server_disconnected = true
    }

    pub fn last_will(&self) -> Option<&LastWill> {
        self.last_will.as_ref()
    }

    pub fn clear_last_will(&mut self) {
        self.last_will = None
    }

    pub fn take_last_will(&mut self) -> Option<LastWill> {
        self.last_will.take()
    }

    pub fn set_last_will(&mut self, last_will: LastWill) {
        self.last_will = Some(last_will);
    }

    pub fn set_clean_session(&mut self, clean_session: bool) {
        self.clean_session = clean_session;
    }

    pub fn subscriptions(&self) -> &HashSet<TopicFilter> {
        &self.subscriptions
    }

    pub fn subscribe(&mut self, topic: TopicFilter) -> bool {
        self.subscriptions.insert(topic)
    }

    pub fn unsubscribe(&mut self, topic: &TopicFilter) -> bool {
        self.subscriptions.remove(topic)
    }

    pub fn incr_server_packet_id(&mut self) -> u16 {
        let old_value = self.server_packet_id;
        self.server_packet_id += 1;
        old_value
    }

    pub fn build_state(&mut self) -> SessionState {
        let mut subscriptions = HashSet::new();
        mem::swap(&mut self.subscriptions, &mut subscriptions);

        SessionState {
            server_packet_id: self.server_packet_id,
            subscriptions,
        }
    }

    pub fn copy_state(&mut self, state: SessionState) {
        self.server_packet_id = state.server_packet_id;
        self.subscriptions = state.subscriptions;
    }
}

pub struct SessionState {
    server_packet_id: u16,
    subscriptions: HashSet<TopicFilter>,
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"client# {} session:
                connect at : {:?}
             clean session : {}
                keep alive : {}
        assigned client id : {}"#,
            self.client_id,
            self.connected_at,
            self.clean_session,
            self.keep_alive,
            self.assigned_client_id
        )
    }
}
