use std::{cmp, io, time::Duration};

use futures::SinkExt as _;
use kanal::AsyncReceiver;
use mqtt_codec_kit::{
    common::{
        MATCH_ALL_STR, MATCH_ONE_STR, QualityOfService, TopicFilter, qos::QoSWithPacketIdentifier,
    },
    v5::{
        control::{
            DisconnectReasonCode, PubackReasonCode, PubcompReasonCode, PubrecReasonCode,
            PubrelReasonCode,
        },
        packet::{
            DisconnectPacket, PingrespPacket, PubackPacket, PubcompPacket, PublishPacket,
            PubrecPacket, PubrelPacket, SubackPacket, SubscribePacket, UnsubackPacket,
            UnsubscribePacket, VariablePacket, suback::SubscribeReasonCode,
            subscribe::RetainHandling, unsuback::UnsubscribeReasonCode,
        },
    },
};
use tokio::{
    io::AsyncWrite,
    time::{Instant, interval_at},
};
use tokio_util::codec::{Encoder, FramedWrite};

use crate::{
    debug, error,
    protocols::{Error, ProtocolSessionState, v5::build_error_disconnect},
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
            VariablePacket::DisconnectPacket(packet) => self.handle_disconnect(packet).await?,
            _ => {
                debug!("invalid packet: {:?}", packet);
                return Err(Error::ServerDisconnect);
            }
        };

        Ok(())
    }

    async fn handle_forwarded_publish(&mut self, packet: ForwardMessage) -> Result<(), Error> {
        match packet {
            ForwardMessage::Publish(topic_filter, subscribe_qos, packet) => {
                debug!(
                    r#"""client#{} receive forwarded packet, topic filter: {:?}, subscribe qos: {:?}, packet: {:?}"""#,
                    self.session.client_id(),
                    topic_filter,
                    subscribe_qos,
                    packet,
                );
                if !self.session.subscriptions().contains_key(&topic_filter) {
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
                    .send(ProtocolSessionState::V5(self.session.build_state()))
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
            r#"client#{} received a publish packet, topic name: {:?}, payload: {:?}, flags: qos={:?}, retain={}, dup={}"#,
            self.session.client_id(),
            packet.topic_name(),
            packet.payload(),
            packet.qos(),
            packet.retain(),
            packet.dup(),
        );

        let message_count = self
            .global
            .storage
            .message_count(self.session.client_id())
            .await?;
        if message_count >= self.session.receive_maximum().into() {
            let err_pkt = build_error_disconnect(
                &self.session,
                DisconnectReasonCode::ReceiveMaximumExceeded,
                "received more than Receive Maximum publication",
            );
            self.writer.send(err_pkt.into()).await?;
            return Err(Error::ServerDisconnect);
        }

        let topic_name = packet.topic_name();
        if topic_name.is_empty()
            || topic_name.contains(MATCH_ALL_STR)
            || topic_name.contains(MATCH_ONE_STR)
        {
            let err_pkt = build_error_disconnect(
                &self.session,
                DisconnectReasonCode::TopicNameInvalid,
                "invalid topic name",
            );
            self.writer.send(err_pkt.into()).await?;
            return Err(Error::ServerDisconnect);
        }

        if packet.qos() == QoSWithPacketIdentifier::Level0 && packet.dup() {
            let err_pkt = build_error_disconnect(
                &self.session,
                DisconnectReasonCode::ProtocolError,
                "invalid duplicate flag in QoS 0 publish message",
            );
            self.writer.send(err_pkt.into()).await?;
            return Err(Error::ServerDisconnect);
        }

        match packet.qos() {
            QoSWithPacketIdentifier::Level0 => {
                self.forward_publish_packet((self.session.client_id().to_owned(), packet).into())
                    .await?;
            }
            QoSWithPacketIdentifier::Level1(packet_id) => {
                if !packet.dup() {
                    self.forward_publish_packet(
                        (self.session.client_id().to_owned(), packet).into(),
                    )
                    .await?;
                }
                self.writer
                    .send(PubackPacket::new(packet_id, PubackReasonCode::Success).into())
                    .await?;
            }
            QoSWithPacketIdentifier::Level2(packet_id) => {
                if !packet.dup() {
                    self.global
                        .storage
                        .save_publish_message(
                            self.session.client_id(),
                            packet_id,
                            (self.session.client_id().to_owned(), packet).into(),
                        )
                        .await?;
                }
                self.writer
                    .send(PubrecPacket::new(packet_id, PubrecReasonCode::Success).into())
                    .await?;
            }
        }
        Ok(())
    }

    async fn forward_publish_packet(&self, packet: PublishMessage) -> Result<(), Error> {
        debug!(
            r#"client#{} forward publish message, topic name: {:?}, payload: {:?}, flags: qos={:?}, retain={}, dup={}"#,
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
            "client#{} received a pubrel packet, id: {}",
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
                .send(
                    PubcompPacket::new(packet.packet_identifier(), PubcompReasonCode::Success)
                        .into(),
                )
                .await?;

            self.forward_publish_packet(msg).await?;
        }
        Ok(())
    }

    async fn handle_puback(&self, packet: PubackPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a puback packet, id: {}",
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
            "client#{} received a pubrec packet, id: {}",
            self.session.client_id(),
            packet.packet_identifier()
        );

        let matched = self
            .global
            .storage
            .pubrec(self.session.client_id(), packet.packet_identifier())
            .await?;

        let pkt = if matched {
            PubrelPacket::new(packet.packet_identifier(), PubrelReasonCode::Success)
        } else {
            PubrelPacket::new(
                packet.packet_identifier(),
                PubrelReasonCode::PacketIdentifierNotFound,
            )
        };
        self.writer.send(pkt.into()).await?;
        Ok(())
    }

    async fn handle_pubcomp(&self, packet: PubcompPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a pubcomp packet, id: {}",
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
            r#"client#{} handle last will, client side disconnected: {}, server side disconnected: {}, last will: {:?}"#,
            self.session.client_id(),
            self.session.client_disconnected(),
            self.session.server_disconnected(),
            self.session.last_will(),
        );

        if let Some(last_will) = self.session.take_last_will() {
            self.forward_publish_packet((self.session.client_id().to_owned(), last_will).into())
                .await?;
        }
        Ok(())
    }

    async fn handle_subscribe(&mut self, packet: SubscribePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} received a subscribe packet, packet id: {}, topics: {:?}"#,
            self.session.client_id(),
            packet.packet_identifier(),
            packet.subscribes(),
        );

        let properties = packet.properties();
        if properties.identifier() == Some(0) {
            let disconnect_packet = build_error_disconnect(
                &self.session,
                DisconnectReasonCode::ProtocolError,
                "Subscription identifier value=0 is not allowed",
            );
            self.writer.send(disconnect_packet.into()).await?;
            return Err(Error::ServerDisconnect);
        }
        if packet.subscribes().is_empty() {
            let disconnect_packet = build_error_disconnect(
                &self.session,
                DisconnectReasonCode::ProtocolError,
                "Subscription is empty",
            );
            self.writer.send(disconnect_packet.into()).await?;
            return Err(Error::ServerDisconnect);
        }
        // TODO: config subscription identifier available false
        // properties.identifier().is_some() && !config.subscription_id_available()

        let mut reason_codes = Vec::with_capacity(packet.subscribes().len());
        let mut pkts = Vec::new();
        for (filter, sub_opts) in packet.subscribes() {
            // TODO: shared subscribe
            // SubscribeReasonCode::SharedSubscriptionNotSupported
            // SubscribeReasonCode::WildcardSubscriptionsNotSupported topic contain +/#

            // TODO: config granted max qos
            let granted_qos = sub_opts.qos().to_owned();
            self.global
                .storage
                .subscribe(self.session.client_id(), filter, granted_qos)
                .await?;
            let exist = self.session.subscribe(filter.clone(), *sub_opts);
            // TODO: config: retain available?
            let send_retain = !filter.is_shared()
                && match sub_opts.retain_handling() {
                    RetainHandling::SendAtSubscribe => true,
                    RetainHandling::SendAtSubscribeIfNotExist => exist,
                    RetainHandling::DoNotSend => false,
                };
            if send_retain {
                let retain_messages =
                    RetainMessageStore::search(self.global.storage.as_ref(), filter).await?;
                for msg in retain_messages {
                    if sub_opts.no_local() && msg.client_id().eq(self.session.client_id()) {
                        continue;
                    }

                    let final_qos = cmp::min(granted_qos, msg.qos());
                    let qos = match final_qos {
                        QualityOfService::Level0 => QoSWithPacketIdentifier::Level0,
                        QualityOfService::Level1 => {
                            QoSWithPacketIdentifier::Level1(self.session.incr_server_packet_id())
                        }
                        QualityOfService::Level2 => {
                            QoSWithPacketIdentifier::Level2(self.session.incr_server_packet_id())
                        }
                    };

                    let mut packet =
                        PublishPacket::new(msg.topic_name().to_owned(), qos, msg.payload());
                    packet.set_dup(msg.dup());
                    if let Some(p) = msg.properties() {
                        packet.set_properties(p.to_owned());
                    }
                    pkts.push(packet);
                }
            }

            let reason_code = match granted_qos {
                QualityOfService::Level0 => SubscribeReasonCode::GrantedQos0,
                QualityOfService::Level1 => SubscribeReasonCode::GrantedQos1,
                QualityOfService::Level2 => SubscribeReasonCode::GrantedQos2,
            };
            reason_codes.push(reason_code);
        }
        self.writer
            .send(SubackPacket::new(packet.packet_identifier(), reason_codes).into())
            .await?;

        for pkt in pkts {
            self.writer.send(pkt.into()).await?;
            // TODO: store qos1/2 msg
        }
        Ok(())
    }

    async fn handle_unsubscribe(&mut self, packet: UnsubscribePacket) -> Result<(), Error> {
        debug!(
            r#"client#{} received a unsubscribe packet, packet id: {}, topics: {:?}"#,
            self.session.client_id(),
            packet.packet_identifier(),
            packet.subscribes(),
        );

        let mut reason_codes = Vec::with_capacity(packet.subscribes().len());
        for filter in packet.subscribes() {
            self.session.unsubscribe(filter);

            match self
                .global
                .storage
                .unsubscribe(self.session.client_id(), filter)
                .await
            {
                Ok(_) => reason_codes.push(UnsubscribeReasonCode::Success),
                Err(err) => {
                    error!("Failed to unsubscribe from topic {:?}: {}", filter, err);
                    reason_codes.push(UnsubscribeReasonCode::ImplementationSpecificError);
                }
            }
        }

        self.writer
            .send(UnsubackPacket::new(packet.packet_identifier(), reason_codes).into())
            .await?;

        Ok(())
    }

    async fn handle_disconnect(&mut self, packet: DisconnectPacket) -> Result<(), Error> {
        debug!(
            "client#{} received a disconnect packet",
            self.session.client_id()
        );

        if let Some(value) = packet.properties().session_expiry_interval() {
            self.session.set_session_expiry_interval(value);
            self.session.set_clean_session(true);
        }

        if packet.reason_code() == DisconnectReasonCode::NormalDisconnection {
            self.session.clear_last_will();
        }
        self.session.set_client_disconnected();
        Err(Error::Disconnect)
    }

    async fn remove_client(&self) -> Result<(), Error> {
        if self.session.clean_session() {
            self.global.remove_client(self.session.client_id());
            for topic_filter in self.session.subscriptions().keys() {
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

    async fn handle_clean_message(&mut self, packet: ForwardMessage) -> Result<bool, Error> {
        match packet {
            ForwardMessage::Publish(topic_filter, subscribe_qos, packet) => {
                debug!(
                    r#"""client#{} receive forward packet, topic filter: {:?}, subscribe qos: {:?}, packet: {:?}"""#,
                    self.session.client_id(),
                    topic_filter,
                    subscribe_qos,
                    packet,
                );
                if !self.session.subscriptions().contains_key(&topic_filter) {
                    return Ok(false);
                }

                let final_qos = cmp::min(packet.qos(), subscribe_qos);
                let (packet_id, qos) = match final_qos {
                    QualityOfService::Level0 => return Ok(false),
                    QualityOfService::Level1 => {
                        let packet_id = self.session.incr_server_packet_id();
                        (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                    }
                    QualityOfService::Level2 => {
                        let packet_id = self.session.incr_server_packet_id();
                        (packet_id, QoSWithPacketIdentifier::Level1(packet_id))
                    }
                };

                let mut properties = None;
                if let Some(p) = packet.properties() {
                    properties = Some(p.to_owned());
                }
                let mut message = PendingPublishMessage::new(qos, *packet);
                message.set_dup(message.dup());
                message.message_mut().set_properties(properties);

                self.global
                    .storage
                    .save_pending_publish_message(self.session.client_id(), packet_id, message)
                    .await?;

                Ok(false)
            }
            ForwardMessage::Online(sender) => {
                debug!("client#{} receive online message", self.session.client_id(),);
                if let Err(err) = sender
                    .send(ProtocolSessionState::V5(self.session.build_state()))
                    .await
                {
                    error!(
                        "client#{} send session state: {err}",
                        self.session.client_id(),
                    );
                }

                self.remove_client().await?;
                Ok(true)
            }
            ForwardMessage::Kick(reason) => {
                debug!(
                    "client#{} receive kick message: {}",
                    self.session.client_id(),
                    reason,
                );
                self.remove_client().await?;
                Ok(true)
            }
        }
    }

    async fn handle_clean_session(&mut self) -> Result<(), Error> {
        debug!(
            r#"client#{} handle clean session, clean session: {}, keep alive: {}, client side disconnected: {}, server side disconnected: {}, session expiry: {}"#,
            self.session.client_id(),
            self.session.clean_session(),
            self.session.keep_alive(),
            self.session.client_disconnected(),
            self.session.server_disconnected(),
            self.session.session_expiry_interval(),
        );

        self.writer.close().await?;
        if self.session.server_disconnected() {
            return Ok(());
        }

        if !self.session.disconnected() {
            self.session.set_server_disconnected();
        }

        if !self.session.client_disconnected() {
            self.handle_will().await?;
        }

        if self.session.session_expiry_interval() > 0 {
            let dur = Duration::from_secs(self.session.session_expiry_interval() as u64);
            let mut tick = interval_at(Instant::now() + dur, dur);

            loop {
                tokio::select! {
                    out = self.forward_rx.recv() => {
                        match out {
                            Ok(p) => if self.handle_clean_message(p).await? {
                                break;
                            },
                            Err(err) => {
                                error!("handle deliver failed: {err}");
                                break;
                            },
                        }
                    }
                    _ = tick.tick() => {
                        debug!("handle clean session client#{} session expired", self.session.client_id());
                        break;
                    }
                }
            }
        } else {
            if self.session.clean_session() {
                self.remove_client().await?;
                return Ok(());
            }
            while let Ok(p) = self.forward_rx.recv().await {
                let stop = self.handle_clean_message(p).await?;
                if stop {
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
                                .send(
                                    PubrelPacket::new(packet_id, PubrelReasonCode::Success).into(),
                                )
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
                            if let Error::ServerDisconnect = err {
                                self.session.set_server_disconnected();
                            }
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
