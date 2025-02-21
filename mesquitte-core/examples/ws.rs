use std::{env, io, sync::OnceLock};

use mesquitte_core::{
    server::{config::ServerConfig, state::GlobalState, ws::server::WsServer},
    store::{
        Storage,
        memory::{
            MemoryStore, message::MessageMemoryStore, retain::RetainMessageMemoryStore,
            topic::TopicMemoryStore,
        },
    },
};

#[tokio::main]
async fn main() -> io::Result<()> {
    unsafe {
        env::set_var("RUST_LOG", "ws=trace,mesquitte_core=trace");
    }
    env_logger::init();

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30, 3);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Storage::new(mem_store);
    let global = GlobalState::new(storage);

    static GLOBAL: OnceLock<GlobalState<MemoryStore>> = OnceLock::new();

    let config = ServerConfig::new("0.0.0.0:8883".parse().unwrap(), None, "4").unwrap();
    let broker = WsServer::new(config, GLOBAL.get_or_init(|| global))
        .await
        .unwrap();
    broker.serve().await.unwrap();
    Ok(())
}
