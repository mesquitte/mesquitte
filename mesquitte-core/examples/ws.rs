use std::{env, io, sync::OnceLock};

use mesquitte_core::{
    server::{config::ServerConfig, state::GlobalState, ws::server::WsServer},
    store::{
        memory::{
            message::MessageMemoryStore, retain::RetainMessageMemoryStore, topic::TopicMemoryStore,
            MemoryStore,
        },
        Storage,
    },
};

#[tokio::main]
async fn main() -> io::Result<()> {
    env::set_var(
        "RUST_LOG",
        "ws=trace,mesquitte_core=trace,mqtt_codec_kit=info",
    );
    env_logger::init();

    let global = GlobalState::default();

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30, 3);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Storage::new(mem_store);

    static GLOBAL: OnceLock<GlobalState> = OnceLock::new();
    static STORAGE: OnceLock<Storage<MemoryStore>> = OnceLock::new();

    let config = ServerConfig::<String>::new("0.0.0.0:8883".parse().unwrap(), None, "4").unwrap();
    let broker = WsServer::bind(
        &config.addr,
        config.clone(),
        GLOBAL.get_or_init(|| global),
        STORAGE.get_or_init(|| storage),
    )
    .await
    .unwrap();
    broker.accept().await.unwrap();
    Ok(())
}
