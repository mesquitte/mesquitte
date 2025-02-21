//! Authenticate Reason Code

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable},
    v5::{control::VariableHeaderError, reason_code_value::*},
};

/// Reason code for `PUBCOMP` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum AuthenticateReasonCode {
    Success,
    ContinueAuthentication,
    ReAuthenticate,
}

impl From<AuthenticateReasonCode> for u8 {
    fn from(value: AuthenticateReasonCode) -> Self {
        match value {
            AuthenticateReasonCode::Success => SUCCESS,
            AuthenticateReasonCode::ContinueAuthentication => CONTINUE_AUTHENTICATION,
            AuthenticateReasonCode::ReAuthenticate => RE_AUTHENTICATE,
        }
    }
}

impl From<&AuthenticateReasonCode> for u8 {
    fn from(value: &AuthenticateReasonCode) -> Self {
        (*value).into()
    }
}

/// Create `AuthenticateReasonCode` from value
impl TryFrom<u8> for AuthenticateReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(AuthenticateReasonCode::Success),
            CONTINUE_AUTHENTICATION => Ok(AuthenticateReasonCode::ContinueAuthentication),
            RE_AUTHENTICATE => Ok(AuthenticateReasonCode::ReAuthenticate),
            v => Err(VariableHeaderError::InvalidAuthenticateReasonCode(v)),
        }
    }
}

impl Encodable for AuthenticateReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.into())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for AuthenticateReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        reader.read_u8().map(Self::try_from)?
    }
}

impl Display for AuthenticateReasonCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code: u8 = self.into();
        write!(f, "{}", code)
    }
}
