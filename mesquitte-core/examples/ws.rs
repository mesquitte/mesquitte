use std::{env, io, sync::Arc};

use mesquitte_core::server::{state::GlobalState, ws::server::WsServer};

#[tokio::main]
async fn main() -> io::Result<()> {
    env::set_var("RUST_LOG", "ws=trace,mesquitte_core=trace,mqtt_codec=info");
    env_logger::init();

    let global = GlobalState::new();

    let broker = WsServer::bind("0.0.0.0:6666".parse().unwrap(), Arc::new(global))
        .await
        .unwrap();
    broker.accept().await.unwrap();
    Ok(())
}
