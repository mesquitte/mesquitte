use std::{env, io, sync::Arc};

use mesquitte_core::server::{state::GlobalState, tcp::server::TcpServer};

#[tokio::main]
async fn main() -> io::Result<()> {
    env::set_var("RUST_LOG", "tcp=trace,lutein_core=trace,mqtt_codec=info");
    env_logger::init();

    let global = Arc::new(GlobalState::new());
    let broker = TcpServer::bind("0.0.0.0:1883".parse().unwrap(), global)
        .await
        .unwrap();
    broker.accept().await.unwrap();
    Ok(())
}
