use std::{env, io, sync::Arc};

use mesquitte_core::{
    server::{state::GlobalState, tcp::server::TcpServer},
    store::{
        memory::{
            message::MessageMemoryStore, retain::RetainMessageMemoryStore, topic::TopicMemoryStore,
        },
        Storage,
    },
};

#[tokio::main]
async fn main() -> io::Result<()> {
    env::set_var(
        "RUST_LOG",
        "tcp=trace,mesquitte_core=trace,mqtt_codec_kit=info",
    );
    env_logger::init();

    let global = Arc::new(GlobalState::new());

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30);
    let retain_message = RetainMessageMemoryStore::default();

    let storage = Arc::new(Storage::new(message_store, retain_message, topic_store));

    let broker = TcpServer::bind("0.0.0.0:1883", global, storage)
        .await
        .unwrap();
    broker.accept().await.unwrap();
    Ok(())
}
