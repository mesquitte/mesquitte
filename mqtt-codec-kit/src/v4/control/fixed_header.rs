//! Fixed header in MQTT

use std::{
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};
#[cfg(all(feature = "v4", feature = "parse"))]
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::common::{Decodable, Encodable};

use super::{packet_type::PacketTypeError, PacketType};

/// Fixed header for each MQTT control packet
///
/// Format:
///
/// ```plain
/// 7                          3                          0
/// +--------------------------+--------------------------+
/// | MQTT Control Packet Type | Flags for each type      |
/// +--------------------------+--------------------------+
/// | Remaining Length ...                                |
/// +-----------------------------------------------------+
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FixedHeader {
    /// Packet Type
    pub packet_type: PacketType,

    /// The Remaining Length is the number of bytes remaining within the current packet,
    /// including data in the variable header and the payload. The Remaining Length does
    /// not include the bytes used to encode the Remaining Length.
    pub remaining_length: u32,
}

impl FixedHeader {
    pub fn new(packet_type: PacketType, remaining_length: u32) -> Self {
        debug_assert!(remaining_length <= 0x0FFF_FFFF);
        Self {
            packet_type,
            remaining_length,
        }
    }

    /// Asynchronously parse a single fixed header from an AsyncRead type, such as a network
    /// socket.
    #[cfg(all(feature = "v4", feature = "parse"))]
    pub async fn parse<A: AsyncRead + Unpin>(rdr: &mut A) -> Result<Self, FixedHeaderError> {
        let type_val = rdr.read_u8().await?;

        let mut remaining_len = 0;
        let mut i = 0;

        loop {
            let byte = rdr.read_u8().await?;

            remaining_len |= (u32::from(byte) & 0x7F) << (7 * i);

            if i >= 4 {
                return Err(FixedHeaderError::MalformedRemainingLength);
            }

            if byte & 0x80 == 0 {
                break;
            } else {
                i += 1;
            }
        }

        match PacketType::try_from(type_val) {
            Ok(packet_type) => Ok(FixedHeader::new(packet_type, remaining_len)),
            Err(PacketTypeError::ReservedType(ty, _)) => {
                Err(FixedHeaderError::ReservedType(ty, remaining_len))
            }
            Err(err) => Err(From::from(err)),
        }
    }
}

impl Encodable for FixedHeader {
    fn encode<W: Write>(&self, wr: &mut W) -> Result<(), io::Error> {
        wr.write_u8(self.packet_type.into())?;

        let mut cur_len = self.remaining_length;
        loop {
            let mut byte = (cur_len & 0x7F) as u8;
            cur_len >>= 7;

            if cur_len > 0 {
                byte |= 0x80;
            }

            wr.write_u8(byte)?;

            if cur_len == 0 {
                break;
            }
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        let rem_size = if self.remaining_length >= 2_097_152 {
            4
        } else if self.remaining_length >= 16_384 {
            3
        } else if self.remaining_length >= 128 {
            2
        } else {
            1
        };
        1 + rem_size
    }
}

impl Decodable for FixedHeader {
    type Error = FixedHeaderError;
    type Cond = ();

    fn decode_with<R: Read>(rdr: &mut R, _rest: ()) -> Result<Self, Self::Error> {
        let type_val = rdr.read_u8()?;
        let remaining_len = {
            let mut cur = 0u32;
            for i in 0.. {
                let byte = rdr.read_u8()?;
                cur |= ((byte as u32) & 0x7F) << (7 * i);

                if i >= 4 {
                    return Err(FixedHeaderError::MalformedRemainingLength);
                }

                if byte & 0x80 == 0 {
                    break;
                }
            }

            cur
        };

        match PacketType::try_from(type_val) {
            Ok(packet_type) => Ok(Self::new(packet_type, remaining_len)),
            Err(PacketTypeError::ReservedType(ty, _)) => {
                Err(FixedHeaderError::ReservedType(ty, remaining_len))
            }
            Err(err) => Err(From::from(err)),
        }
    }
}

impl Display for FixedHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{packet_type: {}, remaining_length: {}}}",
            self.packet_type, self.remaining_length
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FixedHeaderError {
    #[error("malformed remaining length")]
    MalformedRemainingLength,
    #[error("reserved header ({0}, {1})")]
    ReservedType(u8, u32),
    #[error(transparent)]
    PacketTypeError(#[from] PacketTypeError),
    #[error(transparent)]
    IoError(#[from] io::Error),
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::v4::control::packet_type::{ControlType, PacketType};

    use super::*;

    #[test]
    fn test_encode_fixed_header() {
        let header = FixedHeader::new(PacketType::with_default(ControlType::Connect), 321);
        let mut buf = Vec::new();
        header.encode(&mut buf).unwrap();

        let expected = b"\x10\xc1\x02";
        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_decod_fixed_header() {
        let stream = b"\x10\xc1\x02";
        let mut cursor = Cursor::new(&stream[..]);
        let header = FixedHeader::decode(&mut cursor).unwrap();
        assert_eq!(
            header.packet_type,
            PacketType::with_default(ControlType::Connect)
        );
        assert_eq!(header.remaining_length, 321);
    }

    #[test]
    #[should_panic]
    fn test_decode_too_long_fixed_header() {
        let stream = b"\x10\x80\x80\x80\x80\x02";
        let mut cursor = Cursor::new(&stream[..]);
        FixedHeader::decode(&mut cursor).unwrap();
    }

    #[test]
    fn test_display_fixed_header() {
        let header = FixedHeader::new(PacketType::with_default(ControlType::Connect), 321);

        assert_eq!(
            header.to_string(),
            "{packet_type: CONNECT, remaining_length: 321}"
        );
    }
}
