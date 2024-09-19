use std::mem;
use std::sync::Arc;

use hashbrown::HashMap;
use mqtt_codec_kit::common::{PacketIdentifier, QualityOfService, TopicFilter};
use parking_lot::RwLock;
use tokio::time::Instant;

use super::{client_id::ClientId, last_will::LastWill, pending_packets::PendingPackets};

pub const DEFAULT_MAX_PACKET_SIZE: u32 = 5 + 268_435_455;

pub struct Session {
    connected_at: Instant,
    connection_closed_at: Option<Instant>,
    // last package timestamp
    last_packet_at: Arc<RwLock<Instant>>,
    // For record packet id send from server to client
    server_packet_id: PacketIdentifier,

    pending_packets: PendingPackets,

    client_id: ClientId,
    client_identifier: Arc<String>,
    username: Option<Arc<String>>,
    keep_alive: u16,
    clean_session: bool,
    last_will: Option<LastWill>,
    subscribes: HashMap<TopicFilter, QualityOfService, ahash::RandomState>,

    authorized: bool,
    client_disconnected: bool,
    server_disconnected: bool,

    // #[cfg(feature = "v5")]
    assigned_client_id: bool,
    // #[cfg(feature = "v5")]
    server_keep_alive: bool,
    // #[cfg(feature = "v5")]
    session_expiry_interval: u32,
    // #[cfg(feature = "v5")]
    receive_maximum: u16,
    // #[cfg(feature = "v5")]
    max_packet_size: u32,
    // #[cfg(feature = "v5")]
    topic_alias_max: u16,
    // #[cfg(feature = "v5")]
    request_response_info: bool,
    // #[cfg(feature = "v5")]
    request_problem_info: bool,
    // #[cfg(feature = "v5")]
    user_properties: Vec<(String, String)>,
    // #[cfg(feature = "v5")]
    authentication_method: Option<Arc<String>>,
    // #[cfg(feature = "v5")]
    // authentication_data: Option<Arc<String>>,
}

impl Session {
    pub fn new(
        client_id: String,
        max_inflight_client: u16,
        max_in_mem_pending_messages: usize,
        inflight_timeout: u64,
    ) -> Self {
        Self {
            connected_at: Instant::now(),
            connection_closed_at: None,
            last_packet_at: Arc::new(RwLock::new(Instant::now())),
            server_packet_id: PacketIdentifier(1),

            pending_packets: PendingPackets::new(
                max_inflight_client,
                max_in_mem_pending_messages,
                inflight_timeout,
            ),

            client_id: Default::default(),
            client_identifier: Arc::new(client_id),
            username: None,
            keep_alive: 0,
            clean_session: true,
            last_will: None,
            subscribes: HashMap::with_hasher(ahash::RandomState::new()),

            authorized: false,
            client_disconnected: false,
            server_disconnected: false,
            assigned_client_id: false,
            server_keep_alive: false,

            session_expiry_interval: 0,
            receive_maximum: max_inflight_client,
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            // TODO: config: max topic alias
            topic_alias_max: 65535,
            request_response_info: false,
            request_problem_info: true,
            user_properties: Vec::new(),
            authentication_method: None,
        }
    }

    pub fn connected_at(&self) -> &Instant {
        &self.connected_at
    }

    pub fn connection_closed_at(&self) -> Option<&Instant> {
        self.connection_closed_at.as_ref()
    }

    pub fn last_packet_at(&self) -> Arc<RwLock<Instant>> {
        self.last_packet_at.clone()
    }

    pub fn renew_last_packet_at(&self) {
        *self.last_packet_at.write() = Instant::now();
    }

    pub fn pending_packets(&mut self) -> &mut PendingPackets {
        &mut self.pending_packets
    }

    pub fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub fn set_client_id(&mut self, client_id: ClientId) {
        self.client_id = client_id
    }

    pub fn client_identifier(&self) -> Arc<String> {
        self.client_identifier.clone()
    }

    pub fn set_client_identifier(&mut self, client_identifier: &str) {
        self.client_identifier = Arc::new(client_identifier.to_owned())
    }

    pub fn set_username(&mut self, username: Option<Arc<String>>) {
        self.username = username
    }

    pub fn keep_alive(&mut self) -> u16 {
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

    pub fn last_will(&mut self) -> Option<&LastWill> {
        self.last_will.as_ref()
    }

    pub fn clear_last_will(&mut self) {
        self.last_will = None
    }

    pub fn take_last_will(&mut self) -> Option<LastWill> {
        self.last_will.take()
    }

    pub fn set_last_will(&mut self, last_will: Option<LastWill>) {
        self.last_will = last_will;
    }

    pub fn set_clean_session(&mut self, clean_session: bool) {
        self.clean_session = clean_session;
    }

    pub fn subscribes(&self) -> &HashMap<TopicFilter, QualityOfService, ahash::RandomState> {
        &self.subscribes
    }

    pub fn set_subscribe(&mut self, topic: TopicFilter, qos: QualityOfService) {
        self.subscribes.insert(topic, qos);
    }

    pub fn rm_subscribe(&mut self, topic: &TopicFilter) -> bool {
        self.subscribes.remove(topic).is_some()
    }

    pub fn server_packet_id(&self) -> PacketIdentifier {
        self.server_packet_id
    }

    pub fn incr_server_packet_id(&mut self) -> u16 {
        let old_value = self.server_packet_id;
        self.server_packet_id.0 += 1;
        old_value.0
    }

    pub fn assigned_client_id(&self) -> bool {
        self.assigned_client_id
    }

    pub fn set_assigned_client_id(&mut self) {
        self.assigned_client_id = true
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
        self.authentication_method = Some(Arc::new(authentication_method.to_owned()));
    }

    pub fn copy_from_state(&mut self, mut state: SessionState) {
        self.server_packet_id = state.server_packet_id;

        mem::swap(&mut state.pending_packets, &mut self.pending_packets);
        mem::swap(&mut state.subscribes, &mut self.subscribes);
    }
}

pub struct SessionState {
    // For record packet id send from server to client
    server_packet_id: PacketIdentifier,
    // QoS1/QoS2 pending packets
    pending_packets: PendingPackets,

    subscribes: HashMap<TopicFilter, QualityOfService, ahash::RandomState>,
}

impl From<&mut Session> for SessionState {
    fn from(val: &mut Session) -> Self {
        let mut pending_packets = PendingPackets::new(0, 0, 0);
        let mut subscribes = HashMap::with_hasher(ahash::RandomState::new());

        mem::swap(&mut val.pending_packets, &mut pending_packets);
        mem::swap(&mut val.subscribes, &mut subscribes);

        Self {
            server_packet_id: val.server_packet_id,
            pending_packets,
            subscribes,
        }
    }
}
