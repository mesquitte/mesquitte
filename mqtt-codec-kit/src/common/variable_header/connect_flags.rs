use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::common::{Decodable, Encodable};

/// Flags for `CONNECT` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct ConnectFlags {
    pub username: bool,
    pub password: bool,
    pub will_retain: bool,
    pub will_qos: u8,
    pub will_flag: bool,
    pub clean_session: bool,
    // We never use this, but must decode because brokers must verify it's zero per [MQTT-3.1.2-3]
    pub reserved: bool,
}

impl ConnectFlags {
    pub fn empty() -> Self {
        Self {
            username: false,
            password: false,
            will_retain: false,
            will_qos: 0,
            will_flag: false,
            clean_session: false,
            reserved: false,
        }
    }
}

impl Encodable for ConnectFlags {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        let code = ((self.username as u8) << 7)
            | ((self.password as u8) << 6)
            | ((self.will_retain as u8) << 5)
            | ((self.will_qos) << 3)
            | ((self.will_flag as u8) << 2)
            | ((self.clean_session as u8) << 1);

        writer.write_u8(code)
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ConnectFlags {
    type Error = ConnectFlagsError;
    type Cond = ();

    fn decode_with<R: Read>(reader: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        let code = reader.read_u8()?;
        if code & 1 != 0 {
            return Err(ConnectFlagsError::InvalidReservedFlag);
        }

        Ok(Self {
            username: (code & 0b1000_0000) != 0,
            password: (code & 0b0100_0000) != 0,
            will_retain: (code & 0b0010_0000) != 0,
            will_qos: (code & 0b0001_1000) >> 3,
            will_flag: (code & 0b0000_0100) != 0,
            clean_session: (code & 0b0000_0010) != 0,
            reserved: (code & 0b0000_0001) != 0,
        })
    }
}

impl Display for ConnectFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{username: {}, password: {}, will_retain: {}, will_qos: {}, will_flag: {}, clean_session: {}, reserved: {}}}",
            self.username, self.password, self.will_retain, self.will_qos, self.will_flag, self.clean_session, self.reserved
        )
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ConnectFlagsError {
    IoError(#[from] io::Error),
    #[error("invalid reserved flag")]
    InvalidReservedFlag,
}
