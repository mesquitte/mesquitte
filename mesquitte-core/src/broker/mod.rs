use crate::{
    server::{Error, quic::server::QuicServer, tcp::server::TcpServer, ws::server::WsServer},
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
};

#[derive(Default)]
pub struct Broker<S>
where
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    #[cfg(feature = "mqtt")]
    mqtt: Option<TcpServer<S>>,
    #[cfg(feature = "mqtts")]
    mqtts: Option<TcpServer<S>>,
    #[cfg(feature = "ws")]
    ws: Option<WsServer<S>>,
    #[cfg(feature = "wss")]
    wss: Option<WsServer<S>>,
    #[cfg(feature = "quic")]
    quic: Option<QuicServer<S>>,
}

impl<S> Broker<S>
where
    S: MessageStore + RetainMessageStore + TopicStore + 'static,
{
    #[cfg(feature = "mqtt")]
    pub fn with_mqtt(mut self, mqtt: TcpServer<S>) -> Self {
        self.mqtt = Some(mqtt);
        self
    }

    #[cfg(feature = "mqtts")]
    pub fn with_mqtts(mut self, mqtts: TcpServer<S>) -> Self {
        self.mqtts = Some(mqtts);
        self
    }

    #[cfg(feature = "ws")]
    pub fn with_ws(mut self, ws: WsServer<S>) -> Self {
        self.ws = Some(ws);
        self
    }

    #[cfg(feature = "wss")]
    pub fn with_wss(mut self, wss: WsServer<S>) -> Self {
        self.wss = Some(wss);
        self
    }

    #[cfg(feature = "quic")]
    pub fn with_quic(mut self, quic: QuicServer<S>) -> Self {
        self.quic = Some(quic);
        self
    }

    pub async fn serve(self) -> Result<(), Error> {
        #[cfg(feature = "mqtt")]
        tokio::spawn(async {
            self.mqtt.unwrap().serve().await.unwrap();
        });
        #[cfg(feature = "mqtts")]
        tokio::spawn(async {
            self.mqtts.unwrap().serve().await.unwrap();
        });
        #[cfg(feature = "ws")]
        tokio::spawn(async {
            self.ws.unwrap().serve().await.unwrap();
        });
        #[cfg(feature = "wss")]
        tokio::spawn(async {
            self.wss.unwrap().serve().await.unwrap();
        });
        #[cfg(feature = "quic")]
        tokio::spawn(async {
            self.quic.unwrap().serve().await.unwrap();
        });
        Ok(())
    }
}
