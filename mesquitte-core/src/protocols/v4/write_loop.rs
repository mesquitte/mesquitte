use std::{cmp, io, time::Duration};

use futures::SinkExt as _;
use kanal::AsyncReceiver;
use mqtt_codec_kit::{
    common::{
        MATCH_ALL_STR, MATCH_ONE_STR, QualityOfService, SHARED_PREFIX, TopicFilter,
        qos::QoSWithPacketIdentifier,
    },
    v4::packet::{
        DisconnectPacket, PingrespPacket, PubackPacket, PubcompPacket, PublishPacket, PubrecPacket,
        PubrelPacket, SubackPacket, SubscribePacket, UnsubackPacket, UnsubscribePacket,
        VariablePacket, suback::SubscribeReturnCode,
    },
};
use tokio::{
    io::AsyncWrite,
    time::{Instant, interval_at},
};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    debug, error,
    protocols::{Error, ProtocolSessionState},
    server::state::{ForwardMessage, GlobalState},
    store::{
        message::{MessageStore, PendingPublishMessage, PublishMessage},
        retain::RetainMessageStore,
        topic::TopicStore,
    },
    warn,
};

use super::session::Session;

pub(crate) struct WriteLoop<T, E, S: 'static> {
    writer: FramedWrite<T, E>,
    read_rx: AsyncReceiver<VariablePacket>,
    forward_rx: AsyncReceiver<ForwardMessage>,
    session: Session,
    global: &'static GlobalState<S>,
}

impl<T, E, S> WriteLoop<T, E, S>
where
    T: AsyncWrite + Unpin + Send + Sync + 'static,
    E: Encoder<VariablePacket, Error = io::Error> + Send + Sync + 'static,
    S: MessageStore + RetainMessageStore + TopicStore,
{
    pub fn new(
        writer: FramedWrite<T, E>,
        read_rx: AsyncReceiver<VariablePacket>,
        forward_rx: AsyncReceiver<ForwardMessage>,
        session: Session,
        global: &'static GlobalState<S>,
    ) -> Self {
        Self {
            writer,
            read_rx,
            forward_rx,
            session,
            global,
        }
    }

    async fn handle_read_packet(&mut self, packet: VariablePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} read packet: {:?}"#,
            self.session.client_id(),
            packet,
        );

        self.session.renew_last_packet_at();
        match packet {
            VariablePacket::PingreqPacket(_packet) => {
                self.writer.send(PingrespPacket::new().into()).await?;
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

    async fn handle_forwarded_publish(&mut self, packet: ForwardMessage) -> Result<(), Error> {
        match packet {
            ForwardMessage::Publish(topic_filter, subscribe_qos, packet) => {
                debug!(
                    r#"""client#{} receive forwarded packet:
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
                let (p, qos) = match final_qos {
                    QualityOfService::Level0 => (None, QoSWithPacketIdentifier::Level0),
                    QualityOfService::Level1 => {
                        let packet_id = self.session.incr_server_packet_id();
                        (Some(packet_id), QoSWithPacketIdentifier::Level1(packet_id))
                    }
                    QualityOfService::Level2 => {
                        let packet_id = self.session.incr_server_packet_id();
                        (Some(packet_id), QoSWithPacketIdentifier::Level2(packet_id))
                    }
                };

                let publish =
                    PublishPacket::new(packet.topic_name().clone(), qos, packet.payload());
                if let Some(packet_id) = p {
                    self.global
                        .storage
                        .save_pending_publish_message(
                            self.session.client_id(),
                            packet_id,
                            PendingPublishMessage::new(qos, *packet),
                        )
                        .await?;
                }
                self.writer.send(publish.into()).await?;
                Ok(())
            }
            ForwardMessage::Online(sender) => {
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
            ForwardMessage::Kick(reason) => {
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

    async fn handle_publish(&mut self, packet: PublishPacket) -> Result<(), Error> {
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
            self.writer.send(DisconnectPacket::new().into()).await?;
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
            self.writer.send(DisconnectPacket::new().into()).await?;
            return Ok(());
        }
        if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
            debug!(
                "client#{} invalid duplicate flag in QoS 0 publish message",
                self.session.client_id()
            );
            self.writer.send(DisconnectPacket::new().into()).await?;
            return Ok(());
        }

        match packet.qos() {
            QoSWithPacketIdentifier::Level0 => {
                self.forward_publish_packet(packet.into()).await?;
            }
            QoSWithPacketIdentifier::Level1(packet_id) => {
                if !packet.dup() {
                    self.forward_publish_packet(packet.into()).await?;
                }
                self.writer
                    .send(PubackPacket::new(packet_id).into())
                    .await?;
            }
            QoSWithPacketIdentifier::Level2(packet_id) => {
                if !packet.dup() {
                    self.global
                        .storage
                        .save_publish_message(self.session.client_id(), packet_id, packet.into())
                        .await?;
                }
                self.writer
                    .send(PubrecPacket::new(packet_id).into())
                    .await?;
            }
        }
        Ok(())
    }

    async fn forward_publish_packet(&self, packet: PublishMessage) -> Result<(), Error> {
        debug!(
            r#"client#{} forward publish message:
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
                self.global.storage.insert(packet.to_owned()).await?;
            }
        }

        let subscribes = self.global.storage.match_topic(packet.topic_name()).await?;
        for topic_content in subscribes {
            let topic_filter = if let Some(topic_filter) = topic_content.topic_filter {
                match TopicFilter::new(topic_filter) {
                    Ok(filter) => filter,
                    Err(err) => {
                        error!("forward publish message new topic filter: {err}");
                        continue;
                    }
                }
            } else {
                continue;
            };
            for (client_id, subscribe_qos) in topic_content.clients {
                if let Some(sender) = self.global.get_sender(&client_id) {
                    if sender.is_closed() {
                        warn!("client#{:?} forward channel is closed", client_id,);
                        continue;
                    }
                    if let Err(err) = sender
                        .send(ForwardMessage::Publish(
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

    async fn handle_pubrel(&mut self, packet: PubrelPacket) -> Result<(), Error> {
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
            self.writer
                .send(PubcompPacket::new(packet.packet_identifier()).into())
                .await?;

            self.forward_publish_packet(msg).await?;
        }
        Ok(())
    }

    async fn handle_puback(&self, packet: PubackPacket) -> Result<(), Error> {
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

    async fn handle_pubrec(&mut self, packet: PubrecPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a pubrec packet, id : {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        self.global
            .storage
            .pubrec(self.session.client_id(), packet.packet_identifier())
            .await?;
        self.writer
            .send(PubrelPacket::new(packet.packet_identifier()).into())
            .await?;
        Ok(())
    }

    async fn handle_pubcomp(&self, packet: PubcompPacket) -> Result<(), Error> {
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
            self.forward_publish_packet(last_will.into()).await?;
        }
        Ok(())
    }

    async fn handle_subscribe(&mut self, packet: SubscribePacket) -> Result<(), Error> {
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

            // TODO: config granted max qos
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

                let mut msg = PublishPacket::new(msg.topic_name().to_owned(), qos, msg.payload());
                msg.set_retain(true);
                pkts.push(msg);
            }

            return_codes.push(granted_qos.into());
        }
        self.writer
            .send(SubackPacket::new(packet.packet_identifier(), return_codes).into())
            .await?;

        for pkt in pkts {
            self.writer.send(pkt.into()).await?;
            // TODO: store qos1/2 msg
        }
        Ok(())
    }

    async fn handle_unsubscribe(&mut self, packet: UnsubscribePacket) -> Result<(), Error> {
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
        self.writer
            .send(UnsubackPacket::new(packet.packet_identifier()).into())
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

        self.writer.close().await?;
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

        while let Ok(packet) = self.forward_rx.recv().await {
            match packet {
                ForwardMessage::Publish(topic_filter, subscribe_qos, packet) => {
                    debug!(
                        r#"""client#{} receive forward packet:
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
                ForwardMessage::Online(sender) => {
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
                ForwardMessage::Kick(reason) => {
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

    async fn handle_pending_messages(&mut self) -> Result<(), Error> {
        let ret = self
            .global
            .storage
            .get_all_pending_messages(self.session.client_id())
            .await;

        match ret {
            Ok(Some(messages)) => {
                for (packet_id, pending_message) in messages {
                    match pending_message.pubrec_at() {
                        Some(_) => {
                            self.writer
                                .send(PubrelPacket::new(packet_id).into())
                                .await?;
                        }
                        None => {
                            let pkt: PublishPacket = pending_message.into();
                            self.writer.send(pkt.into()).await?;
                        }
                    }
                }
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(err) => Err(Error::Io(err)),
        }
    }

    pub async fn write_to_client(mut self)
    where
        T: AsyncWrite + Unpin,
        E: Encoder<VariablePacket, Error = io::Error>,
    {
        self.handle_pending_messages().await.unwrap();

        let mut keep_alive = if self.session.keep_alive() > 0 {
            let half_interval = Duration::from_millis(self.session.keep_alive() as u64 * 500);
            Some((
                interval_at(Instant::now() + half_interval, half_interval),
                half_interval * 3,
            ))
        } else {
            None
        };
        loop {
            tokio::select! {
                res = self.read_rx.recv() => match res {
                    Ok(pkt) =>   {
                        if let Err(err) = self.handle_read_packet(pkt).await {
                            error!("handle read packet: {}", err);
                            break;
                        }
                    },
                    Err(err) => {
                        error!("receive read packet: {}", err);
                        break;
                    },
                },
                res = self.forward_rx.recv() => match res {
                    Ok(pkt) => {
                        if let Err(err) = self.handle_forwarded_publish(pkt).await {
                            error!("handle forward packet: {}", err);
                            break;
                        }
                    },
                    Err(err) => {
                        error!("receive forward packet: {}", err);
                        break;
                    },
                },
                Some(dur) = async {
                    if let Some((ticker, timeout)) = keep_alive.as_mut() {
                        ticker.tick().await;
                        Some(timeout)
                    } else {
                        None
                    }
                } => {
                    if &self.session.last_packet_at().elapsed() > dur {
                        break;
                    }
                },
            }
        }

        tokio::spawn(async move {
            if let Err(err) = self.handle_clean_session().await {
                error!("handle clean session: {err}");
            }
        });
    }
}
