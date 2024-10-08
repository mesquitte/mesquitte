use std::io::{self, Read, Write};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::common::{Decodable, Encodable};

/// Flags in `CONNACK` packet
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub struct ConnackFlags {
    pub session_present: bool,
}

impl ConnackFlags {
    pub fn empty() -> ConnackFlags {
        ConnackFlags {
            session_present: false,
        }
    }
}

impl Encodable for ConnackFlags {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        let code = self.session_present as u8;
        writer.write_u8(code)
    }

    fn encoded_length(&self) -> u32 {
        1
    }
}

impl Decodable for ConnackFlags {
    type Error = ConnectAckFlagsError;
    type Cond = ();

    fn decode_with<R: Read>(
        reader: &mut R,
        _rest: (),
    ) -> Result<ConnackFlags, ConnectAckFlagsError> {
        let code = reader.read_u8()?;
        if code & !1 != 0 {
            return Err(ConnectAckFlagsError::InvalidReservedFlag);
        }

        Ok(ConnackFlags {
            session_present: code == 1,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub enum ConnectAckFlagsError {
    IoError(#[from] io::Error),
    #[error("invalid reserved flag")]
    InvalidReservedFlag,
}
