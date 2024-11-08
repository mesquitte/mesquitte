use std::{env, sync::OnceLock};

use mesquitte_core::{
    broker::Broker,
    server::{
        config::{ServerConfig, TlsConfig},
        quic::server::QuicServer,
        state::GlobalState,
        tcp::server::TcpServer,
        ws::server::WsServer,
    },
    store::{
        memory::{
            message::MessageMemoryStore, retain::RetainMessageMemoryStore, topic::TopicMemoryStore,
            MemoryStore,
        },
        Storage,
    },
};
use tokio::signal;

#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "broker=trace,mesquitte_core=trace");
    env_logger::init();

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30, 3);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Storage::new(mem_store);
    let global = GlobalState::new(storage);

    static GLOBAL: OnceLock<GlobalState<MemoryStore>> = OnceLock::new();
    let _ = GLOBAL.set(global);

    let config = ServerConfig::new("0.0.0.0:1883".parse().unwrap(), None, "4").unwrap();
    let mqtt = TcpServer::new(config, GLOBAL.get().unwrap()).await.unwrap();
    let config = ServerConfig::new("0.0.0.0:8883".parse().unwrap(), None, "4").unwrap();
    let ws = WsServer::new(config, GLOBAL.get().unwrap()).await.unwrap();
    let tls = TlsConfig::new(
        None,
        "mesquitte-core/examples/certs/cert.pem".parse().unwrap(),
        "mesquitte-core/examples/certs/key.pem".parse().unwrap(),
        false,
    );
    let config = ServerConfig::new("0.0.0.0:6883".parse().unwrap(), Some(tls), "4").unwrap();
    let quic = QuicServer::new(config, GLOBAL.get().unwrap()).unwrap();
    let broker = Broker::<MemoryStore>::default()
        .with_mqtt(mqtt)
        .with_ws(ws)
        .with_quic(quic);
    broker.serve().await.unwrap();
    signal::ctrl_c().await.expect("failed to listen for event");
}
