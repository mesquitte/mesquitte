use std::{env, sync::OnceLock};

use mesquitte_core::{
    server::{
        config::{ServerConfig, TlsConfig},
        quic::server::QuicServer,
        state::GlobalState,
    },
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
    env::set_var(
        "RUST_LOG",
        "quic=trace,mesquitte_core=trace,mqtt_codec_kit=info",
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

    let tls = TlsConfig::new(
        None,
        "mesquitte-core/examples/certs/cert.pem",
        "mesquitte-core/examples/certs/key.pem",
        false,
    );
    let config = ServerConfig::new("0.0.0.0:1883".parse().unwrap(), Some(tls), "4").unwrap();
    let broker = QuicServer::new(
        config,
        GLOBAL.get_or_init(|| global),
        STORAGE.get_or_init(|| storage),
    )
    .unwrap();
    broker.run().await.unwrap();
}
