use std::{env, path::Path, sync::Arc};

use mesquitte_core::server::{quic::server::QuicServer, state::GlobalState};

#[tokio::main]
async fn main() {
    env::set_var(
        "RUST_LOG",
        "quic=trace,mesquitte_core=trace,mqtt_codec_kit=info",
    );
    env_logger::init();

    let global = Arc::new(GlobalState::new());
    let broker = QuicServer::bind(
        "0.0.0.0:1883",
        (
            Path::new("mesquitte-core/examples/certs/cert.pem"),
            Path::new("mesquitte-core/examples/certs/key.pem"),
        ),
        global,
    )
    .unwrap();
    broker.accept().await.unwrap();
}
