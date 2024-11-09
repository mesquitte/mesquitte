//! CONNECT

use std::io::{self, Read, Write};

use crate::{
    common::{
        encodable::VarBytes,
        packet::DecodablePacket,
        protocol_level::SPEC_3_1_1,
        topic_name::{TopicNameDecodeError, TopicNameError},
        ConnectFlags, ConnectFlagsError, Decodable, Encodable, KeepAlive, ProtocolLevel,
        ProtocolName, QualityOfService, TopicName,
    },
    v4::{
        control::{variable_header::VariableHeaderError, ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// `CONNECT` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ConnectPacket {
    fixed_header: FixedHeader,
    protocol_name: ProtocolName,
    protocol_level: ProtocolLevel,
    flags: ConnectFlags,
    keep_alive: KeepAlive,
    payload: ConnectPacketPayload,
}

encodable_packet!(ConnectPacket(
    protocol_name,
    protocol_level,
    flags,
    keep_alive,
    payload
));

impl ConnectPacket {
    pub fn new<C>(client_identifier: C) -> Self
    where
        C: Into<String>,
    {
        Self::with_level("MQTT", client_identifier, SPEC_3_1_1)
            .expect("SPEC_3_1_1 should always be valid")
    }

    pub fn with_level<P, C>(
        protoname: P,
        client_identifier: C,
        level: u8,
    ) -> Result<Self, VariableHeaderError>
    where
        P: Into<String>,
        C: Into<String>,
    {
        let protocol_level = ProtocolLevel::try_from(level)?;
        let mut pkt = Self {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Connect), 0),
            protocol_name: ProtocolName(protoname.into()),
            protocol_level,
            flags: ConnectFlags::empty(),
            keep_alive: KeepAlive(0),
            payload: ConnectPacketPayload::new(client_identifier.into()),
        };

        pkt.fix_header_remaining_len();

        Ok(pkt)
    }

    pub fn set_keep_alive(&mut self, keep_alive: u16) {
        self.keep_alive = KeepAlive(keep_alive);
    }

    pub fn set_username(&mut self, username: Option<String>) {
        self.flags.username = username.is_some();
        self.payload.username = username;
        self.fix_header_remaining_len();
    }

    pub fn set_will(&mut self, will_message: Option<LastWill>) {
        self.flags.will_flag = will_message.is_some();

        self.payload.last_will = will_message;

        self.fix_header_remaining_len();
    }

    pub fn set_password(&mut self, password: Option<String>) {
        self.flags.password = password.is_some();
        self.payload.password = password;
        self.fix_header_remaining_len();
    }

    pub fn set_client_identifier<I: Into<String>>(&mut self, id: I) {
        self.payload.client_identifier = id.into();
        self.fix_header_remaining_len();
    }

    pub fn set_will_retain(&mut self, will_retain: bool) {
        self.flags.will_retain = will_retain;

        if let Some(will) = &self.payload.last_will {
            let mut last_will = will.clone();
            last_will.retain = will_retain;
            self.payload.last_will = Some(last_will);
        }
    }

    /// If the provided parameter is not valid, it will be set to the default value: QualityOfService::Level0.
    pub fn set_will_qos(&mut self, will_qos: u8) {
        let qos = match will_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => QualityOfService::Level0,
        };

        self.flags.will_qos = qos as u8;

        if let Some(will) = &self.payload.last_will {
            let mut last_will = will.clone();

            last_will.qos = qos;
            self.payload.last_will = Some(last_will);
        }
    }

    pub fn set_clean_session(&mut self, clean_session: bool) {
        self.flags.clean_session = clean_session;
    }

    pub fn username(&self) -> Option<&str> {
        self.payload.username.as_ref().map(|x| &x[..])
    }

    pub fn password(&self) -> Option<&str> {
        self.payload.password.as_ref().map(|x| &x[..])
    }

    pub fn will(&self) -> Option<LastWill> {
        self.payload.last_will.clone()
    }

    pub fn will_retain(&self) -> bool {
        self.flags.will_retain
    }

    /// If the provided parameter is not valid, it will be set to the default value: QualityOfService::Level0.
    pub fn will_qos(&self) -> QualityOfService {
        match self.flags.will_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => QualityOfService::Level0,
        }
    }

    pub fn client_identifier(&self) -> &str {
        &self.payload.client_identifier[..]
    }

    pub fn protocol_name(&self) -> &str {
        &self.protocol_name.0
    }

    pub fn protocol_level(&self) -> ProtocolLevel {
        self.protocol_level
    }

    pub fn clean_session(&self) -> bool {
        self.flags.clean_session
    }

    pub fn keep_alive(&self) -> u16 {
        self.keep_alive.0
    }

    /// Read back the "reserved" Connect flag bit 0. For compliant implementations this should
    /// always be false.
    pub fn reserved_flag(&self) -> bool {
        self.flags.reserved
    }
}

impl DecodablePacket for ConnectPacket {
    type DecodePacketError = ConnectPacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let protoname: ProtocolName = Decodable::decode(reader)?;
        let protocol_level: ProtocolLevel =
            Decodable::decode(reader).map_err(VariableHeaderError::InvalidProtocolLevel)?;
        let flags: ConnectFlags = Decodable::decode(reader).map_err(|err| match err {
            ConnectFlagsError::IoError(err) => VariableHeaderError::IoError(err),
            ConnectFlagsError::InvalidReservedFlag => VariableHeaderError::InvalidReservedFlag,
        })?;
        let keep_alive: KeepAlive = Decodable::decode(reader)?;
        let payload: ConnectPacketPayload =
            Decodable::decode_with(reader, Some(flags)).map_err(PacketError::PayloadError)?;

        Ok(Self {
            fixed_header,
            protocol_name: protoname,
            protocol_level,
            flags,
            keep_alive,
            payload,
        })
    }
}

/// Payloads for connect packet
#[derive(Debug, Eq, PartialEq, Clone)]
struct ConnectPacketPayload {
    client_identifier: String,
    last_will: Option<LastWill>,
    username: Option<String>,
    password: Option<String>,
}

impl ConnectPacketPayload {
    pub fn new(client_identifier: String) -> Self {
        ConnectPacketPayload {
            client_identifier,
            last_will: None,
            username: None,
            password: None,
        }
    }
}

impl Encodable for ConnectPacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        self.client_identifier.encode(writer)?;

        if let Some(will) = &self.last_will {
            will.topic.encode(writer)?;
            will.message.encode(writer)?;
        }

        if let Some(ref username) = self.username {
            username.encode(writer)?;
        }

        if let Some(ref password) = self.password {
            password.encode(writer)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.client_identifier.encoded_length()
            + self
                .last_will
                .as_ref()
                .map(|w| w.encoded_length())
                .unwrap_or(0)
            + self
                .username
                .as_ref()
                .map(|t| t.encoded_length())
                .unwrap_or(0)
            + self
                .password
                .as_ref()
                .map(|t| t.encoded_length())
                .unwrap_or(0)
    }
}

impl Decodable for ConnectPacketPayload {
    type Error = ConnectPacketError;
    type Cond = Option<ConnectFlags>;

    fn decode_with<R: Read>(
        reader: &mut R,
        rest: Option<ConnectFlags>,
    ) -> Result<Self, Self::Error> {
        let mut need_will = false;
        let mut need_username = false;
        let mut need_password = false;
        let mut will_retain = false;
        let mut will_qos = 0;

        if let Some(r) = rest {
            need_will = r.will_flag;
            need_username = r.username;
            need_password = r.password;
            will_retain = r.will_retain;
            will_qos = r.will_qos;
        }

        let qos = match will_qos {
            0 => QualityOfService::Level0,
            1 => QualityOfService::Level1,
            2 => QualityOfService::Level2,
            _ => return Err(ConnectPacketError::InvalidQualityOfService),
        };

        let ident = String::decode(reader)?;
        let will = if need_will {
            let topic = TopicName::decode(reader).map_err(|err| match err {
                TopicNameDecodeError::IoError(err) => ConnectPacketError::from(err),
                TopicNameDecodeError::InvalidTopicName(err) => err.into(),
            })?;
            let msg = VarBytes::decode(reader)?;
            Some(LastWill {
                topic,
                message: msg,
                retain: will_retain,
                qos,
            })
        } else {
            None
        };
        let uname = if need_username {
            Some(String::decode(reader)?)
        } else {
            None
        };
        let pwd = if need_password {
            Some(String::decode(reader)?)
        } else {
            None
        };

        Ok(Self {
            client_identifier: ident,
            last_will: will,
            username: uname,
            password: pwd,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ConnectPacketError {
    IoError(#[from] io::Error),
    TopicNameError(#[from] TopicNameError),
    #[error("invalid quality of service")]
    InvalidQualityOfService,
}

// LastWill
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LastWill {
    topic: TopicName,
    message: VarBytes,
    qos: QualityOfService,
    retain: bool,
}

impl LastWill {
    pub fn new<S: Into<String>>(topic: S, msg: Vec<u8>) -> Result<Self, ConnectPacketError> {
        Ok(Self {
            topic: TopicName::new(topic)?,
            message: VarBytes(msg),
            qos: QualityOfService::Level0,
            retain: false,
        })
    }

    pub fn topic(&self) -> &TopicName {
        &self.topic
    }

    pub fn message(&self) -> &VarBytes {
        &self.message
    }

    pub fn qos(&self) -> QualityOfService {
        self.qos
    }

    pub fn retain(&self) -> bool {
        self.retain
    }
}

impl Encodable for LastWill {
    fn encode<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        self.topic.encode(writer)?;
        self.message.encode(writer)?;

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        2 + self.topic.encoded_length() + 2 + self.message.encoded_length()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    use crate::common::{Decodable, Encodable};

    #[test]
    fn test_connect_packet_encode_basic() {
        let packet = ConnectPacket::new("12345".to_owned());
        let expected = b"\x10\x11\x00\x04MQTT\x04\x00\x00\x00\x00\x0512345";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_connect_packet_decode_basic() {
        let encoded_data = b"\x10\x11\x00\x04MQTT\x04\x00\x00\x00\x00\x0512345";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = ConnectPacket::decode(&mut buf).unwrap();

        let expected = ConnectPacket::new("12345".to_owned());
        assert_eq!(expected, packet);
    }

    #[test]
    fn test_connect_packet_username() {
        let mut packet = ConnectPacket::new("12345".to_owned());
        packet.set_username(Some("mqtt_player".to_owned()));

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = ConnectPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded_packet);
    }
}
