//! SUBACK

use std::{
    cmp::Ordering,
    fmt::Display,
    io::{self, Read, Write},
};

use byteorder::{ReadBytesExt, WriteBytesExt};

use crate::{
    common::{Decodable, Encodable, PacketIdentifier, QualityOfService, packet::DecodablePacket},
    v4::{
        control::{ControlType, FixedHeader, PacketType},
        packet::PacketError,
    },
};

/// Subscribe code
#[repr(u8)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum SubscribeReturnCode {
    MaximumQoSLevel0 = 0x00,
    MaximumQoSLevel1 = 0x01,
    MaximumQoSLevel2 = 0x02,
    Failure = 0x80,
}

impl PartialOrd for SubscribeReturnCode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use self::SubscribeReturnCode::*;
        match (self, other) {
            (&Failure, _) => None,
            (_, &Failure) => None,
            (&MaximumQoSLevel0, &MaximumQoSLevel0) => Some(Ordering::Equal),
            (&MaximumQoSLevel1, &MaximumQoSLevel1) => Some(Ordering::Equal),
            (&MaximumQoSLevel2, &MaximumQoSLevel2) => Some(Ordering::Equal),
            (&MaximumQoSLevel0, _) => Some(Ordering::Less),
            (&MaximumQoSLevel1, &MaximumQoSLevel0) => Some(Ordering::Greater),
            (&MaximumQoSLevel1, &MaximumQoSLevel2) => Some(Ordering::Less),
            (&MaximumQoSLevel2, _) => Some(Ordering::Greater),
        }
    }
}

impl From<QualityOfService> for SubscribeReturnCode {
    fn from(qos: QualityOfService) -> Self {
        match qos {
            QualityOfService::Level0 => SubscribeReturnCode::MaximumQoSLevel0,
            QualityOfService::Level1 => SubscribeReturnCode::MaximumQoSLevel1,
            QualityOfService::Level2 => SubscribeReturnCode::MaximumQoSLevel2,
        }
    }
}

impl Display for SubscribeReturnCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", *self as u8)
    }
}

/// `SUBACK` packet
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct SubackPacket {
    fixed_header: FixedHeader,
    packet_identifier: PacketIdentifier,
    payload: SubackPacketPayload,
}

encodable_packet!(SubackPacket(packet_identifier, payload));

impl SubackPacket {
    pub fn new(pkid: u16, return_codes: Vec<SubscribeReturnCode>) -> Self {
        let mut pkt = Self {
            fixed_header: FixedHeader::new(
                PacketType::with_default(ControlType::SubscribeAcknowledgement),
                0,
            ),
            packet_identifier: PacketIdentifier(pkid),
            payload: SubackPacketPayload::new(return_codes),
        };
        pkt.fix_header_remaining_len();
        pkt
    }

    pub fn packet_identifier(&self) -> u16 {
        self.packet_identifier.0
    }

    pub fn set_packet_identifier(&mut self, pkid: u16) {
        self.packet_identifier.0 = pkid;
    }

    pub fn return_codes(&self) -> &[SubscribeReturnCode] {
        &self.payload.return_codes[..]
    }
}

impl DecodablePacket for SubackPacket {
    type DecodePacketError = SubackPacketError;
    type F = FixedHeader;
    type Error = PacketError<Self>;

    fn decode_packet<R: Read>(reader: &mut R, fixed_header: Self::F) -> Result<Self, Self::Error> {
        let packet_identifier = PacketIdentifier::decode(reader)?;
        let payload: SubackPacketPayload = SubackPacketPayload::decode_with(
            reader,
            fixed_header.remaining_length - packet_identifier.encoded_length(),
        )
        .map_err(PacketError::PayloadError)?;
        Ok(Self {
            fixed_header,
            packet_identifier,
            payload,
        })
    }
}

impl Display for SubackPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{fixed_header: {}, packet_identifier: {}, payload: {}}}",
            self.fixed_header, self.packet_identifier, self.payload
        )
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct SubackPacketPayload {
    return_codes: Vec<SubscribeReturnCode>,
}

impl SubackPacketPayload {
    pub fn new(codes: Vec<SubscribeReturnCode>) -> Self {
        Self {
            return_codes: codes,
        }
    }
}

impl Encodable for SubackPacketPayload {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        for code in self.return_codes.iter() {
            writer.write_u8(*code as u8)?;
        }

        Ok(())
    }

    fn encoded_length(&self) -> u32 {
        self.return_codes.len() as u32
    }
}

impl Decodable for SubackPacketPayload {
    type Error = SubackPacketError;
    type Cond = u32;

    fn decode_with<R: Read>(reader: &mut R, payload_len: u32) -> Result<Self, Self::Error> {
        let mut codes = Vec::new();

        for _ in 0..payload_len {
            let return_code = match reader.read_u8()? {
                0x00 => SubscribeReturnCode::MaximumQoSLevel0,
                0x01 => SubscribeReturnCode::MaximumQoSLevel1,
                0x02 => SubscribeReturnCode::MaximumQoSLevel2,
                0x80 => SubscribeReturnCode::Failure,
                code => return Err(SubackPacketError::InvalidSubscribeReturnCode(code)),
            };

            codes.push(return_code);
        }

        Ok(Self::new(codes))
    }
}

impl Display for SubackPacketPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{return_codes: [")?;
        let mut iter = self.return_codes.iter();
        if let Some(first) = iter.next() {
            write!(f, "{}", first)?;
            for code in iter {
                write!(f, ", {}", code)?;
            }
        }
        write!(f, "]}}")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubackPacketError {
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error("invalid subscribe return code {0}")]
    InvalidSubscribeReturnCode(u8),
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use crate::common::encodable::Encodable;

    use super::*;

    #[test]
    fn test_suback_packet_encode_hex() {
        let packet = SubackPacket::new(
            40303,
            vec![
                SubscribeReturnCode::MaximumQoSLevel1,
                SubscribeReturnCode::MaximumQoSLevel1,
            ],
        );

        let expected = b"\x90\x04\x9d\x6f\x01\x01";

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        assert_eq!(&expected[..], &buf[..]);
    }

    #[test]
    fn test_suback_packet_decode_hex() {
        let encoded_data = b"\x90\x04\x9d\x6c\x00\x00";

        let mut buf = Cursor::new(&encoded_data[..]);
        let packet = SubackPacket::decode(&mut buf).unwrap();

        let expected = SubackPacket::new(
            40300,
            vec![
                SubscribeReturnCode::MaximumQoSLevel0,
                SubscribeReturnCode::MaximumQoSLevel0,
            ],
        );

        assert_eq!(expected, packet);
    }

    #[test]
    fn test_suback_packet_basic() {
        let subscribes = vec![SubscribeReturnCode::MaximumQoSLevel0];

        let packet = SubackPacket::new(10001, subscribes);

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let mut decode_buf = Cursor::new(buf);
        let decoded = SubackPacket::decode(&mut decode_buf).unwrap();

        assert_eq!(packet, decoded);
    }

    #[test]
    fn test_display_suback_packet() {
        let return_codes = vec![SubscribeReturnCode::MaximumQoSLevel1];
        let packet = SubackPacket::new(123, return_codes);

        assert_eq!(
            packet.to_string(),
            "{fixed_header: {packet_type: SUBACK, remaining_length: 3}, packet_identifier: 123, payload: {return_codes: [1]}}"
        );
    }
}
