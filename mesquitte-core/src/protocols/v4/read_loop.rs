use std::{cmp, time::Duration};

use futures::StreamExt as _;
use kanal::{AsyncReceiver, AsyncSender};
use mqtt_codec_kit::{
    common::{
        qos::QoSWithPacketIdentifier, QualityOfService, TopicFilter, MATCH_ALL_STR, MATCH_ONE_STR,
        SHARED_PREFIX,
    },
    v4::packet::{
        suback::SubscribeReturnCode, DisconnectPacket, PingrespPacket, PubackPacket, PubcompPacket,
        PublishPacket, PubrecPacket, PubrelPacket, SubackPacket, SubscribePacket, UnsubackPacket,
        UnsubscribePacket, VariablePacket, VariablePacketError,
    },
};
use tokio::{
    io::AsyncRead,
    time::{interval_at, Instant},
};
use tokio_util::codec::{Decoder, FramedRead};

use crate::{
    debug, error,
    protocols::{Error, ProtocolSessionState},
    server::state::{DeliverMessage, GlobalState},
    store::{
        message::{MessageStore, PendingPublishMessage, PublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
    },
    warn,
};

use super::{session::Session, WritePacket};

pub(crate) struct ReadLoop<T, D, S: 'static> {
    reader: FramedRead<T, D>,
    write_tx: AsyncSender<WritePacket>,
    deliver_rx: AsyncReceiver<DeliverMessage>,
    session: Session,
    global: &'static GlobalState<S>,
}

impl<T, D, S> ReadLoop<T, D, S>
where
    T: AsyncRead + Unpin + Send + Sync + 'static,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError> + Send + Sync + 'static,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        reader: FramedRead<T, D>,
        session: Session,
        deliver_rx: AsyncReceiver<DeliverMessage>,
        write_tx: AsyncSender<WritePacket>,
        global: &'static GlobalState<S>,
    ) -> Self {
        Self {
            reader,
            session,
            deliver_rx,
            write_tx,
            global,
        }
    }

    pub async fn read_from_client(mut self) {
        if self.session.keep_alive() > 0 {
            let half_interval = Duration::from_millis(self.session.keep_alive() as u64 * 500);
            let mut keep_alive_tick = interval_at(Instant::now() + half_interval, half_interval);
            let keep_alive_timeout = half_interval * 3;
            loop {
                tokio::select! {
                    packet = self.reader.next() => match packet {
                        Some(Ok(p)) => match self.handle_read_packet(&p).await {
                            Ok(_) => continue,
                            Err(err) => {
                                warn!("handle read packet error: {err}");
                                break;
                            }
                        },
                        Some(Err(err)) => {
                            error!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            error!("reader closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(packet) => match self.handle_deliver_packet(packet).await {
                            Ok(_) => continue,
                            Err(err) => {
                                warn!("handle deliver failed: {err}");
                                break;
                            }
                        },
                        Err(err) => {
                            warn!("deliver receive channel: {err}");
                            break;
                        }
                    },
                    _ = keep_alive_tick.tick() => {
                        if self.session.last_packet_at().elapsed() > keep_alive_timeout {
                            break;
                        }
                    },
                }
            }
        } else {
            loop {
                tokio::select! {
                    packet = self.reader.next() => match packet {
                        Some(Ok(p)) => match self.handle_read_packet(&p).await {
                            Ok(_) => continue,
                            Err(err) => {
                                warn!("handle read packet error: {err}");
                                break;
                            }
                        },
                        Some(Err(err)) => {
                            error!("read form client failed: {err}");
                            break;
                        }
                        None => {
                            error!("reader closed");
                            break;
                        }
                    },
                    packet = self.deliver_rx.recv() => match packet {
                        Ok(packet) => match self.handle_deliver_packet(packet).await {
                            Ok(_) => continue,
                            Err(err) => {
                                warn!("handle deliver failed: {err}");
                                break;
                            }
                        },
                        Err(err) => {
                            warn!("deliver receive channel: {err}");
                            break;
                        }
                    },
                }
            }
        };

        tokio::spawn(async move {
            if let Err(err) = self.handle_clean_session().await {
                error!("handle clean session: {err}");
            }
        });
    }

    async fn handle_read_packet(&mut self, packet: &VariablePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} read packet: {:?}"#,
            self.session.client_id(),
            packet,
        );

        self.session.renew_last_packet_at();
        match packet {
            VariablePacket::PingreqPacket(_packet) => {
                self.write_tx
                    .send(WritePacket::VariablePacket(PingrespPacket::new().into()))
                    .await?;
            }
            VariablePacket::PublishPacket(packet) => self.handle_publish(packet).await?,
            VariablePacket::PubrelPacket(packet) => self.handle_pubrel(packet).await?,
            VariablePacket::PubackPacket(packet) => self.handle_puback(packet).await?,
            VariablePacket::PubrecPacket(packet) => self.handle_pubrec(packet).await?,
            VariablePacket::SubscribePacket(packet) => self.handle_subscribe(packet).await?,
            VariablePacket::PubcompPacket(packet) => self.handle_pubcomp(packet).await?,
            VariablePacket::UnsubscribePacket(packet) => self.handle_unsubscribe(packet).await?,
            VariablePacket::DisconnectPacket(_packet) => self.handle_disconnect().await?,
            _ => {
                debug!("invalid packet: {:?}", packet);
                return Err(Error::V4InvalidPacket);
            }
        };

        Ok(())
    }

    async fn handle_deliver_packet(&mut self, packet: DeliverMessage) -> Result<(), Error> {
        match packet {
            DeliverMessage::Publish(topic_filter, subscribe_qos, packet) => {
                debug!(
                    r#"""client#{} receive deliver packet:
                         topic filter : {:?},
                        subscribe qos : {:?},
                               packet : {:?}"""#,
                    self.session.client_id(),
                    topic_filter,
                    subscribe_qos,
                    packet,
                );
                if !self.session.subscriptions().contains(&topic_filter) {
                    return Err(Error::Topic(topic_filter.to_string()));
                }
                let final_qos = cmp::min(packet.qos(), subscribe_qos);
                let qos = match final_qos {
                    QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                    QualityOfService::Level1 => {
                        QoSWithPacketIdentifier::Level1(self.session.incr_server_packet_id())
                    }
                    QualityOfService::Level2 => {
                        QoSWithPacketIdentifier::Level2(self.session.incr_server_packet_id())
                    }
                };
                self.write_tx
                    .send(WritePacket::PendingMessage(PendingPublishMessage::new(
                        qos, *packet,
                    )))
                    .await?;
                Ok(())
            }
            DeliverMessage::Online(sender) => {
                debug!("client#{} receive online message", self.session.client_id(),);
                if let Err(err) = sender
                    .send(ProtocolSessionState::V4(self.session.build_state()))
                    .await
                {
                    error!(
                        "client#{} send session state: {err}",
                        self.session.client_id()
                    );
                }

                self.remove_client().await?;
                Err(Error::DupClient(self.session.client_id().to_string()))
            }
            DeliverMessage::Kick(reason) => {
                debug!(
                    "client#{} receive kick message: {}",
                    self.session.client_id(),
                    reason,
                );
                self.remove_client().await?;
                Err(Error::Kick(self.session.client_id().to_string()))
            }
        }
    }

    async fn handle_publish(&self, packet: &PublishPacket) -> Result<(), Error> {
        debug!(
            r#"client#{} received a publish packet:
                topic name : {:?}
                   payload : {:?}
                     flags : qos={:?}, retain={}, dup={}"#,
            self.session.client_id(),
            packet.topic_name(),
            packet.payload(),
            packet.qos(),
            packet.retain(),
            packet.dup(),
        );

        let topic_name = packet.topic_name();
        if topic_name.is_empty() {
            debug!("Publish topic name cannot be empty");
            self.write_tx
                .send(WritePacket::VariablePacket(DisconnectPacket::new().into()))
                .await?;
            return Ok(());
        }

        if topic_name.starts_with(SHARED_PREFIX)
            || topic_name.contains(MATCH_ALL_STR)
            || topic_name.contains(MATCH_ONE_STR)
        {
            debug!(
                "client#{} invalid topic name: {:?}",
                self.session.client_id(),
                topic_name
            );
            self.write_tx
                .send(WritePacket::VariablePacket(DisconnectPacket::new().into()))
                .await?;
            return Ok(());
        }
        if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
            debug!(
                "client#{} invalid duplicate flag in QoS 0 publish message",
                self.session.client_id()
            );
            self.write_tx
                .send(WritePacket::VariablePacket(DisconnectPacket::new().into()))
                .await?;
            return Ok(());
        }

        match packet.qos() {
            QoSWithPacketIdentifier::Level0 => {
                self.deliver_publish_message(&packet.into()).await?;
            }
            QoSWithPacketIdentifier::Level1(packet_id) => {
                if !packet.dup() {
                    self.deliver_publish_message(&packet.into()).await?;
                }
                self.write_tx
                    .send(WritePacket::VariablePacket(
                        PubackPacket::new(packet_id).into(),
                    ))
                    .await?;
            }
            QoSWithPacketIdentifier::Level2(packet_id) => {
                if !packet.dup() {
                    self.global
                        .storage
                        .save_publish_message(self.session.client_id(), packet_id, packet.into())
                        .await?;
                }
                self.write_tx
                    .send(WritePacket::VariablePacket(
                        PubrecPacket::new(packet_id).into(),
                    ))
                    .await?;
            }
        }
        Ok(())
    }

    async fn deliver_publish_message(&self, packet: &PublishMessage) -> Result<(), Error> {
        debug!(
            r#"client#{} deliver publish message:
                topic name : {:?}
                   payload : {:?}
                     flags : qos={:?}, retain={}, dup={}"#,
            self.session.client_id(),
            packet.topic_name(),
            packet.payload(),
            packet.qos(),
            packet.retain(),
            packet.dup(),
        );

        if packet.retain() {
            if packet.payload().is_empty() {
                self.global.storage.remove(packet.topic_name()).await?;
            } else {
                self.global
                    .storage
                    .insert((self.session.client_id(), packet).into())
                    .await?;
            }
        }

        let subscribes = self.global.storage.match_topic(packet.topic_name()).await?;
        for topic_content in subscribes {
            let topic_filter = if let Some(topic_filter) = topic_content.topic_filter {
                match TopicFilter::new(topic_filter) {
                    Ok(filter) => filter,
                    Err(err) => {
                        error!("deliver publish message new topic filter: {err}");
                        continue;
                    }
                }
            } else {
                continue;
            };
            for (client_id, subscribe_qos) in topic_content.clients {
                if let Some(sender) = self.global.get_deliver(&client_id) {
                    if sender.is_closed() {
                        warn!("client#{:?} deliver channel is closed", client_id,);
                        continue;
                    }
                    if let Err(err) = sender
                        .send(DeliverMessage::Publish(
                            topic_filter.clone(),
                            subscribe_qos,
                            Box::new(packet.clone()),
                        ))
                        .await
                    {
                        error!("{} send publish: {}", client_id, err,)
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_pubrel(&self, packet: &PubrelPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a pubrel packet, id : {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        if let Some(msg) = self
            .global
            .storage
            .pubrel(self.session.client_id(), packet.packet_identifier())
            .await?
        {
            self.deliver_publish_message(&msg).await?;
        }
        self.write_tx
            .send(WritePacket::VariablePacket(
                PubcompPacket::new(packet.packet_identifier()).into(),
            ))
            .await?;
        Ok(())
    }

    async fn handle_puback(&self, packet: &PubackPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a puback packet, id : {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        self.global
            .storage
            .puback(self.session.client_id(), packet.packet_identifier())
            .await?;

        Ok(())
    }

    async fn handle_pubrec(&self, packet: &PubrecPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a pubrec packet, id : {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        self.global
            .storage
            .pubrec(self.session.client_id(), packet.packet_identifier())
            .await?;
        self.write_tx
            .send(WritePacket::VariablePacket(
                PubrelPacket::new(packet.packet_identifier()).into(),
            ))
            .await?;
        Ok(())
    }

    async fn handle_pubcomp(&self, packet: &PubcompPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a pubcomp packet, id : {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        self.global
            .storage
            .pubcomp(self.session.client_id(), packet.packet_identifier())
            .await?;

        Ok(())
    }

    async fn handle_will(&mut self) -> Result<(), Error> {
        debug!(
            r#"client#{} handle last will:
                client side disconnected : {}
                server side disconnected : {}
                               last will : {:?}"#,
            self.session.client_id(),
            self.session.client_disconnected(),
            self.session.server_disconnected(),
            self.session.last_will(),
        );

        if let Some(last_will) = self.session.take_last_will() {
            self.deliver_publish_message(&last_will.into()).await?;
        }
        Ok(())
    }

    async fn handle_subscribe(&mut self, packet: &SubscribePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} received a subscribe packet:
                packet id : {}
                   topics : {:?}"#,
            self.session.client_id(),
            packet.packet_identifier(),
            packet.subscribes(),
        );
        if packet.subscribes().is_empty() {
            return Err(Error::EmptySubscribes);
        }
        let mut return_codes = Vec::with_capacity(packet.subscribes().len());
        let mut pkts = Vec::new();
        for (filter, subscribe_qos) in packet.subscribes() {
            if filter.is_shared() {
                warn!("mqtt v3.x don't support shared subscription");
                return_codes.push(SubscribeReturnCode::Failure);
                continue;
            }

            // TODO: granted max qos from config
            let granted_qos = subscribe_qos.to_owned();
            self.global
                .storage
                .subscribe(self.session.client_id(), filter, granted_qos)
                .await?;
            self.session.subscribe(filter.clone());
            let retain_messages =
                RetainMessageStore::search(self.global.storage.as_ref(), filter).await?;
            for msg in retain_messages {
                let qos = match granted_qos {
                    QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                    QualityOfService::Level1 => {
                        QoSWithPacketIdentifier::Level1(self.session.incr_server_packet_id())
                    }
                    QualityOfService::Level2 => {
                        QoSWithPacketIdentifier::Level2(self.session.incr_server_packet_id())
                    }
                };
                let mut received_publish: PublishMessage = msg.into();
                received_publish.set_retain(true);

                let pending_message = PendingPublishMessage::new(qos, received_publish);
                pkts.push(WritePacket::PendingMessage(pending_message));
            }

            return_codes.push(granted_qos.into());
        }
        self.write_tx
            .send(WritePacket::VariablePacket(
                SubackPacket::new(packet.packet_identifier(), return_codes).into(),
            ))
            .await?;
        for pkt in pkts {
            self.write_tx.send(pkt).await?;
        }
        Ok(())
    }

    async fn handle_unsubscribe(&mut self, packet: &UnsubscribePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} received a unsubscribe packet:
                    packet id : {}
                       topics : {:?}"#,
            self.session.client_id(),
            packet.packet_identifier(),
            packet.topic_filters(),
        );
        for filter in packet.topic_filters() {
            self.session.unsubscribe(filter);
            self.global
                .storage
                .unsubscribe(self.session.client_id(), filter)
                .await?;
        }
        self.write_tx
            .send(WritePacket::VariablePacket(
                UnsubackPacket::new(packet.packet_identifier()).into(),
            ))
            .await?;
        Ok(())
    }

    async fn handle_disconnect(&mut self) -> Result<(), Error> {
        debug!(
            "client#{} received a disconnect packet",
            self.session.client_id()
        );

        self.session.clear_last_will();
        self.session.set_client_disconnected();
        Err(Error::Disconnect)
    }

    async fn remove_client(&self) -> Result<(), Error> {
        if self.session.clean_session() {
            self.global.remove_client(self.session.client_id());
            for topic_filter in self.session.subscriptions() {
                self.global
                    .storage
                    .unsubscribe(self.session.client_id(), topic_filter)
                    .await?;
            }
            self.global
                .storage
                .clear_all(self.session.client_id())
                .await?;
        }
        Ok(())
    }

    async fn handle_clean_session(&mut self) -> Result<(), Error> {
        debug!(
            r#"client#{} handle clean session:
                    clean session : {}
                       keep alive : {}"#,
            self.session.client_id(),
            self.session.clean_session(),
            self.session.keep_alive(),
        );
        if !self.session.disconnected() {
            self.session.set_server_disconnected();
        }

        if !self.session.client_disconnected() {
            self.handle_will().await?;
        }

        if self.session.clean_session() {
            self.remove_client().await?;
            return Ok(());
        }

        while let Ok(packet) = self.deliver_rx.recv().await {
            match packet {
                DeliverMessage::Publish(topic_filter, subscribe_qos, packet) => {
                    debug!(
                        r#"""client#{} receive deliver packet:
                                 topic filter : {:?},
                                subscribe qos : {:?},
                                       packet : {:?}"""#,
                        self.session.client_id(),
                        topic_filter,
                        subscribe_qos,
                        packet,
                    );
                    if !self.session.subscriptions().contains(&topic_filter) {
                        continue;
                    }
                    let final_qos = cmp::min(packet.qos(), subscribe_qos);
                    let (packet_id, qos) = match final_qos {
                        QualityOfService::Level0 => continue,
                        QualityOfService::Level1 => {
                            let packet_id = self.session.incr_server_packet_id();
                            (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                        }
                        QualityOfService::Level2 => {
                            let packet_id = self.session.incr_server_packet_id();
                            (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                        }
                    };

                    let message = PendingPublishMessage::new(qos, *packet);
                    self.global
                        .storage
                        .save_pending_publish_message(self.session.client_id(), packet_id, message)
                        .await?;
                }
                DeliverMessage::Online(sender) => {
                    debug!("client#{} receive online message", self.session.client_id(),);
                    if let Err(err) = sender
                        .send(ProtocolSessionState::V4(self.session.build_state()))
                        .await
                    {
                        error!(
                            "client#{} send session state: {err}",
                            self.session.client_id(),
                        );
                    }

                    self.remove_client().await?;
                    break;
                }
                DeliverMessage::Kick(reason) => {
                    debug!(
                        "client#{} receive kick message: {}",
                        self.session.client_id(),
                        reason,
                    );
                    self.remove_client().await?;
                    break;
                }
            }
        }
        Ok(())
    }
}
