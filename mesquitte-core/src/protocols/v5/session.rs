use std::{fmt, mem};

use foldhash::{HashMap, HashMapExt};
use mqtt_codec_kit::{
    common::TopicFilter,
    v5::packet::{connect::LastWill, subscribe::SubscribeOptions},
};
use tokio::time::Instant;

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 5 + 268_435_455;

pub(super) struct Session {
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
    subscriptions: HashMap<TopicFilter, SubscribeOptions>,

    authorized: bool,
    assigned_client_id: bool,
    client_disconnected: bool,
    server_disconnected: bool,

    server_keep_alive: bool,
    session_expiry_interval: u32,
    receive_maximum: u16,
    max_packet_size: u32,
    topic_alias_max: u16,
    request_response_info: bool,
    request_problem_info: bool,
    user_properties: Vec<(String, String)>,
    authentication_method: Option<String>,
    // authentication_data: Option<Arc<String>>,
}

impl Session {
    pub fn new(client_id: String, assigned_client_id: bool, receive_maximum: u16) -> Self {
        Self {
            connected_at: Instant::now(),
            last_packet_at: Instant::now(),
            server_packet_id: 1,

            client_id,
            assigned_client_id,
            username: None,
            keep_alive: 0,
            clean_session: true,
            last_will: None,
            subscriptions: HashMap::new(),

            authorized: false,
            client_disconnected: false,
            server_disconnected: false,
            server_keep_alive: false,

            session_expiry_interval: 0,
            receive_maximum,
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            // TODO: config: max topic alias
            topic_alias_max: 65535,
            request_response_info: false,
            request_problem_info: true,
            user_properties: Vec::new(),
            authentication_method: None,
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
        // MQTT V5
        // MQTT V3?
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

    pub fn authorized(&self) -> bool {
        self.authorized
    }

    pub fn set_authorized(&mut self, authorized: bool) {
        self.authorized = authorized;
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

    pub fn set_server_keep_alive(&mut self, server_keep_alive: bool) {
        self.server_keep_alive = server_keep_alive
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

    pub fn subscriptions(&self) -> &HashMap<TopicFilter, SubscribeOptions> {
        &self.subscriptions
    }

    pub fn subscribe(&mut self, topic: TopicFilter, options: SubscribeOptions) -> bool {
        self.subscriptions.insert(topic, options).is_some()
    }

    pub fn unsubscribe(&mut self, topic: &TopicFilter) {
        self.subscriptions.remove(topic);
    }

    pub fn incr_server_packet_id(&mut self) -> u16 {
        let old_value = self.server_packet_id;
        self.server_packet_id += 1;
        old_value
    }

    pub fn assigned_client_id(&self) -> bool {
        self.assigned_client_id
    }

    pub fn server_keep_alive(&self) -> bool {
        self.server_keep_alive
    }

    pub fn session_expiry_interval(&self) -> u32 {
        self.session_expiry_interval
    }

    pub fn set_session_expiry_interval(&mut self, session_expiry_interval: u32) {
        self.session_expiry_interval = session_expiry_interval;
    }

    pub fn receive_maximum(&self) -> u16 {
        self.receive_maximum
    }

    pub fn set_receive_maximum(&mut self, receive_maximum: u16) {
        self.receive_maximum = receive_maximum;
    }

    pub fn max_packet_size(&self) -> u32 {
        self.max_packet_size
    }

    pub fn set_max_packet_size(&mut self, max_packet_size: u32) {
        if max_packet_size < self.max_packet_size {
            self.max_packet_size = max_packet_size;
        }
    }

    pub fn topic_alias_max(&self) -> u16 {
        self.topic_alias_max
    }

    pub fn set_topic_alias_max(&mut self, topic_alias_max: u16) {
        self.topic_alias_max = topic_alias_max;
    }

    pub fn request_response_info(&self) -> bool {
        self.request_response_info
    }

    pub fn set_request_response_info(&mut self, request_response_info: bool) {
        self.request_response_info = request_response_info;
    }

    pub fn request_problem_info(&self) -> bool {
        self.request_problem_info
    }

    pub fn set_request_problem_info(&mut self, request_problem_info: bool) {
        self.request_problem_info = request_problem_info;
    }

    pub fn user_properties(&self) -> &Vec<(String, String)> {
        self.user_properties.as_ref()
    }

    pub fn set_user_properties(&mut self, user_properties: Vec<(String, String)>) {
        self.user_properties = user_properties;
    }

    pub fn set_authentication_method(&mut self, authentication_method: &str) {
        self.authentication_method = Some(authentication_method.to_owned());
    }

    pub fn build_state(&mut self) -> SessionState {
        let mut subscriptions = HashMap::new();
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
    subscriptions: HashMap<TopicFilter, SubscribeOptions>,
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
