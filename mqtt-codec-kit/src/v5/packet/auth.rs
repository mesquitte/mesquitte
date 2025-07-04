use std::fmt::Display;

use crate::{
    common::{Decodable, packet::DecodablePacket},
    v5::control::{
        AuthProperties, AuthenticateReasonCode, ControlType, FixedHeader, PacketType,
        VariableHeaderError,
    },
};

use super::PacketError;

/// `AUTH` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct AuthPacket {
    fixed_header: FixedHeader,
    reason_code: AuthenticateReasonCode,
    properties: Option<AuthProperties>,
}

encodable_packet!(AuthPacket(reason_code, properties));

impl AuthPacket {
    pub fn new(reason_code: AuthenticateReasonCode) -> Self {
        if reason_code == AuthenticateReasonCode::Success {
            return AuthPacket {
                fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Auth), 0),
                reason_code,
                properties: None,
            };
        }
        let mut pkt = AuthPacket {
            fixed_header: FixedHeader::new(PacketType::with_default(ControlType::Auth), 0),
            reason_code,
            properties: Some(AuthProperties::default()),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn new_success() -> Self {
        Self::new(AuthenticateReasonCode::Success)
    }

    pub fn set_properties(&mut self, properties: Option<AuthProperties>) {
        self.properties = properties;
        self.fix_header_remaining_len();
    }

    pub fn properties(&self) -> &Option<AuthProperties> {
        &self.properties
    }

    pub fn reason_code(&self) -> AuthenticateReasonCode {
        self.reason_code
    }
}

impl DecodablePacket for AuthPacket {
    type DecodePacketError = std::convert::Infallible;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: std::io::Read>(
        reader: &mut R,
        fixed_header: Self::F,
    ) -> Result<Self, Self::Error> {
        let auth = if fixed_header.remaining_length == 0 {
            Self {
                fixed_header,
                reason_code: AuthenticateReasonCode::Success,
                properties: None,
            }
        } else {
            let reason_code = AuthenticateReasonCode::decode(reader)?;
            let properties =
                AuthProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
            Self {
                fixed_header,
                reason_code,
                properties: Some(properties),
            }
        };
        Ok(auth)
    }
}

impl Display for AuthPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, reason_code: {}",
            self.fixed_header, self.reason_code
        )?;
        if let Some(properties) = &self.properties {
            write!(f, ", properties: {properties}")?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::{Decodable, Encodable};

    use super::*;

    #[test]
    fn test_auth_packet_basic() {
        let packet = AuthPacket::new_success();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = AuthPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_auth_packet_with_reason() {
        let packet = AuthPacket::new(AuthenticateReasonCode::ContinueAuthentication);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = AuthPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_auth_packet_with_properties() {
        let mut packet = AuthPacket::new(AuthenticateReasonCode::ReAuthenticate);

        let mut properties = AuthProperties::default();
        properties.set_reason_string(Some("ReAuthenticate".to_string()));
        properties.add_user_property("foo", "bar");

        packet.set_properties(Some(properties));

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = AuthPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_auth_packet() {
        let packet = AuthPacket::new(AuthenticateReasonCode::ContinueAuthentication);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: AUTH, remaining_length: 2}, reason_code: 24, properties: {reason_string: None, user_properties: [], authentication_method: None, authentication_data: None}}"
        );
    }

    #[test]
    fn test_display_auth_packet_with_properties() {
        let mut packet = AuthPacket::new(AuthenticateReasonCode::ContinueAuthentication);

        let mut properties = AuthProperties::default();
        properties.set_reason_string(Some("Next Stage".to_owned()));

        packet.set_properties(Some(properties));

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: AUTH, remaining_length: 15}, reason_code: 24, properties: {reason_string: Next Stage, user_properties: [], authentication_method: None, authentication_data: None}}"
        );
    }
}
