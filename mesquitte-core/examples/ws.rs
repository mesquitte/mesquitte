use std::{env, io, sync::Arc};

use mesquitte_core::{
    server::{state::GlobalState, ws::server::WsServer},
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

    let global = Arc::new(GlobalState::default());

    let topic_store = TopicMemoryStore::default();
    let message_store = MessageMemoryStore::new(102400, 30);
    let retain_message_store = RetainMessageMemoryStore::default();

    let mem_store = MemoryStore::new(message_store, retain_message_store, topic_store);
    let storage = Arc::new(Storage::new(mem_store));

    let broker = WsServer::bind("0.0.0.0:6666", global, storage)
        .await
        .unwrap();
    broker.accept().await.unwrap();
    Ok(())
}
