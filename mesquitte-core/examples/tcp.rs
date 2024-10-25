use std::{env, sync::OnceLock};

use log::info;
use mesquitte_core::{
    server::{config::ServerConfig, state::GlobalState, tcp::server::TcpServer},
    store::{
        memory::{
            message::MessageMemoryStore, retain::RetainMessageMemoryStore, topic::TopicMemoryStore,
            MemoryStore,
        },
        Storage,
    },
};

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "tcp=trace,mesquitte_core=trace");
    env_logger::init();

    let global = GlobalState::default();

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30, 3);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Storage::new(mem_store);

    static GLOBAL: OnceLock<GlobalState> = OnceLock::new();
    static STORAGE: OnceLock<Storage<MemoryStore>> = OnceLock::new();

    let config = ServerConfig::new("0.0.0.0:1883".parse().unwrap(), None, "4").unwrap();
    info!("server config: {:?}", config);
    let broker = TcpServer::new(
        config,
        GLOBAL.get_or_init(|| global),
        STORAGE.get_or_init(|| storage),
    )
    .await
    .unwrap();
    broker.serve().await.unwrap();
}
