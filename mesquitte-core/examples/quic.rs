use std::{env, sync::OnceLock};

use mesquitte_core::{
    server::{
        config::{ServerConfig, TlsConfig},
        quic::server::QuicServer,
        state::GlobalState,
    },
    store::{
        Storage,
        memory::{
            MemoryStore, message::MessageMemoryStore, retain::RetainMessageMemoryStore,
            topic::TopicMemoryStore,
        },
    },
};

#[tokio::main]
async fn main() {
    unsafe {
        env::set_var("RUST_LOG", "quic=trace,mesquitte_core=trace");
    }
    env_logger::init();

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30, 3);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Storage::new(mem_store);
    let global = GlobalState::new(storage);

    static GLOBAL: OnceLock<GlobalState<MemoryStore>> = OnceLock::new();

    let tls = TlsConfig::new(
        None,
        "mesquitte-core/examples/certs/cert.pem".parse().unwrap(),
        "mesquitte-core/examples/certs/key.pem".parse().unwrap(),
        false,
    );
    let config = ServerConfig::new("0.0.0.0:1883".parse().unwrap(), Some(tls), "4").unwrap();
    let broker = QuicServer::new(config, GLOBAL.get_or_init(|| global)).unwrap();
    broker.serve().await.unwrap();
}
