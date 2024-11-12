use std::io;

use futures::SinkExt as _;
use kanal::AsyncReceiver;
use mqtt_codec_kit::{
    common::qos::QoSWithPacketIdentifier,
    v4::packet::{PublishPacket, VariablePacket},
};
use tokio::io::AsyncWrite;
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    error,
    server::state::GlobalState,
    store::{message::MessageStore, retain::RetainMessageStore, topic::TopicStore},
    warn,
};

use super::WritePacket;

pub(crate) struct WriteLoop<T, E, S: 'static> {
    writer: FramedWrite<T, E>,
    client_id: String,
    write_rx: AsyncReceiver<WritePacket>,
    global: &'static GlobalState<S>,
}

impl<T, E, S> WriteLoop<T, E, S>
where
    T: AsyncWrite + Unpin,
    E: Encoder<VariablePacket, Error = io::Error>,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        client_id: String,
        write_rx: AsyncReceiver<WritePacket>,
        global: &'static GlobalState<S>,
    ) -> Self {
        Self {
            writer,
            write_rx,
            client_id,
            global,
        }
    }

    pub async fn write_to_client(&mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        // TODO: config: resend interval
        loop {
            match self.write_rx.recv().await {
                Ok(message) => match message {
                    WritePacket::VariablePacket(pkt) => {
                        if let Err(err) = self.writer.send(pkt).await {
                            warn!("client#{} write failed: {}", self.client_id, err);
                            break;
                        }
                    }
                    WritePacket::PendingMessage(pending_message) => {
                        let pkt: PublishPacket = (&pending_message).into();
                        if let Err(err) = self.writer.send(pkt.into()).await {
                            warn!("client#{} write failed: {}", self.client_id, err);
                            break;
                        }

                        let packet_id = match pending_message.qos() {
                            QoSWithPacketIdentifier::Level1(packet_id) => packet_id,
                            QoSWithPacketIdentifier::Level2(packet_id) => packet_id,
                            _ => continue,
                        };
                        if let Err(err) = self
                            .global
                            .storage
                            .save_pending_publish_message(
                                &self.client_id,
                                packet_id,
                                pending_message,
                            )
                            .await
                        {
                            error!("save pending publish message: {err}");
                            break;
                        }
                    }
                },
                Err(err) => {
                    error!("client#{} write channel: {err}", self.client_id);
                    break;
                }
            }
        }
    }
}
