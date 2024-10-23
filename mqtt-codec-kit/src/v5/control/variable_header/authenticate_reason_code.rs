//! Authenticate Reason Code

use std::io::{self, Read, Write};

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

impl AuthenticateReasonCode {
    /// Get the value
    pub fn to_u8(self) -> u8 {
        match self {
            AuthenticateReasonCode::Success => SUCCESS,
            AuthenticateReasonCode::ContinueAuthentication => CONTINUE_AUTHENTICATION,
            AuthenticateReasonCode::ReAuthenticate => RE_AUTHENTICATE,
        }
    }
}

/// Create `AuthenticateReasonCode` from value
impl TryFrom<u8> for AuthenticateReasonCode {
    type Error = VariableHeaderError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            SUCCESS => Ok(Self::Success),
            CONTINUE_AUTHENTICATION => Ok(Self::ContinueAuthentication),
            RE_AUTHENTICATE => Ok(Self::ReAuthenticate),
            v => Err(VariableHeaderError::InvalidAuthenticateReasonCode(v)),
        }
    }
}

impl Encodable for AuthenticateReasonCode {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        writer.write_u8(self.to_u8())
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for AuthenticateReasonCode {
    type Error = VariableHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<AuthenticateReasonCode, VariableHeaderError> {
        reader
            .read_u8()
            .map(AuthenticateReasonCode::try_from)?
            .map_err(From::from)
    }
}
