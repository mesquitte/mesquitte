use crate::{
    common::{packet::DecodablePacket, Decodable},
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
            AuthPacket {
                fixed_header,
                reason_code: AuthenticateReasonCode::Success,
                properties: None,
            }
        } else {
            let reason_code = AuthenticateReasonCode::decode(reader)?;
            let properties =
                AuthProperties::decode(reader).map_err(VariableHeaderError::PropertyTypeError)?;
            AuthPacket {
                fixed_header,
                reason_code,
                properties: Some(properties),
            }
        };
        Ok(auth)
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::{Decodable, Encodable};

    use super::*;

    #[test]
    pub fn test_auth_packet_basic() {
        let packet = AuthPacket::new_success();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = AuthPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_auth_packet_with_reason() {
        let packet = AuthPacket::new(AuthenticateReasonCode::ContinueAuthentication);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = AuthPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    pub fn test_auth_packet_with_properties() {
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
}
