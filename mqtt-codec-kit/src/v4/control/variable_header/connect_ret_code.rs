use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v4::control::variable_header::VariableHeaderError,
};

pub const CONNECTION_ACCEPTED: u8 = 0x00;
pub const UNACCEPTABLE_PROTOCOL_VERSION: u8 = 0x01;
pub const IDENTIFIER_REJECTED: u8 = 0x02;
pub const SERVICE_UNAVAILABLE: u8 = 0x03;
pub const BAD_USERNAME_OR_PASSWORD: u8 = 0x04;
pub const NOT_AUTHORIZED: u8 = 0x05;

/// Return code for `CONNACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum ConnectReturnCode {
    ConnectionAccepted,
    UnacceptableProtocolVersion,
    IdentifierRejected,
    ServiceUnavailable,
    BadUserNameOrPassword,
    NotAuthorized,
    Reserved(u8),
}

impl From<ConnectReturnCode> for u8 {
    fn from(value: ConnectReturnCode) -> Self {
        match value {
            ConnectReturnCode::ConnectionAccepted => CONNECTION_ACCEPTED,
            ConnectReturnCode::UnacceptableProtocolVersion => UNACCEPTABLE_PROTOCOL_VERSION,
            ConnectReturnCode::IdentifierRejected => IDENTIFIER_REJECTED,
            ConnectReturnCode::ServiceUnavailable => SERVICE_UNAVAILABLE,
            ConnectReturnCode::BadUserNameOrPassword => BAD_USERNAME_OR_PASSWORD,
            ConnectReturnCode::NotAuthorized => NOT_AUTHORIZED,
            ConnectReturnCode::Reserved(r) => r,
        }
    }
}

impl From<&ConnectReturnCode> for u8 {
    fn from(value: &ConnectReturnCode) -> Self {
        (*value).into()
    }
}

impl From<u8> for ConnectReturnCode {
    /// Create `ConnectReturnCode` from code
    fn from(code: u8) -> Self {
        match code {
            CONNECTION_ACCEPTED => ConnectReturnCode::ConnectionAccepted,
            UNACCEPTABLE_PROTOCOL_VERSION => ConnectReturnCode::UnacceptableProtocolVersion,
            IDENTIFIER_REJECTED => ConnectReturnCode::IdentifierRejected,
            SERVICE_UNAVAILABLE => ConnectReturnCode::ServiceUnavailable,
            BAD_USERNAME_OR_PASSWORD => ConnectReturnCode::BadUserNameOrPassword,
            NOT_AUTHORIZED => ConnectReturnCode::NotAuthorized,
            _ => ConnectReturnCode::Reserved(code),
        }
    }
}

impl Encodable for ConnectReturnCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ConnectReturnCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::from).map_err(From::from)
    }
}
