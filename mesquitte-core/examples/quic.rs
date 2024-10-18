use std::{env, path::Path, sync::Arc};

use mesquitte_core::{
    server::{quic::server::QuicServer, state::GlobalState},
    store::{
        memory::{
            message::MessageMemoryStore, retain::RetainMessageMemoryStore, topic::TopicMemoryStore,
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

    let global = Arc::new(GlobalState::new());

    let topic_store = TopicMemoryStore::new();
    let message_store = MessageMemoryStore::new(102400, 30);
    let retain_message = RetainMessageMemoryStore::new();

    let storage = Arc::new(Storage::new(message_store, retain_message, topic_store));

    let broker = QuicServer::bind(
        "0.0.0.0:1883",
        (
            Path::new("mesquitte-core/examples/certs/cert.pem"),
            Path::new("mesquitte-core/examples/certs/key.pem"),
        ),
        global,
        storage,
    )
    .unwrap();
    broker.accept().await.unwrap();
}
