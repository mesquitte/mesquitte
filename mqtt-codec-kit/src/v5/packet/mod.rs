//! Specific packets

use std::{
    fmt::{self, Debug},
    io::{self, Read, Write},
};

#[cfg(all(feature = "v5", feature = "parse"))]
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{
    common::{
        packet::{DecodablePacket, EncodablePacket},
        Decodable, TopicNameDecodeError, TopicNameError,
    },
    v5::control::{
        fixed_header::FixedHeaderError, variable_header::VariableHeaderError, ControlType,
        FixedHeader,
    },
};

macro_rules! encodable_packet {
    ($typ:ident($($field:ident),* $(,)?)) => {
        impl $crate::v5::packet::EncodablePacket for $typ {
            type Output = $crate::v5::control::fixed_header::FixedHeader;

            fn fixed_header(&self) -> &Self::Output {
                &self.fixed_header
            }

            #[allow(unused_variables)]
            fn encode_packet<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
                $($crate::common::Encodable::encode(&self.$field, writer)?;)*
                Ok(())
            }

            fn encoded_packet_length(&self) -> u32 {
                $($crate::common::Encodable::encoded_length(&self.$field) +)*
                    0
            }
        }

        impl $typ {
            #[allow(dead_code)]
            #[inline(always)]
            fn fix_header_remaining_len(&mut self) {
                self.fixed_header.remaining_length = $crate::v5::packet::EncodablePacket::encoded_packet_length(self);
            }
        }
    };
}

pub use self::{
    auth::AuthPacket,
    connack::ConnackPacket,
    connect::ConnectPacket,
    disconnect::DisconnectPacket,
    pingreq::PingreqPacket,
    pingresp::PingrespPacket,
    puback::PubackPacket,
    pubcomp::PubcompPacket,
    publish::{PublishPacket, PublishPacketRef},
    pubrec::PubrecPacket,
    pubrel::PubrelPacket,
    suback::SubackPacket,
    subscribe::SubscribePacket,
    unsuback::UnsubackPacket,
    unsubscribe::UnsubscribePacket,
};

pub mod auth;
pub mod connack;
pub mod connect;
pub mod disconnect;
pub mod pingreq;
pub mod pingresp;
pub mod puback;
pub mod pubcomp;
pub mod publish;
pub mod pubrec;
pub mod pubrel;
pub mod suback;
pub mod subscribe;
pub mod unsuback;
pub mod unsubscribe;

macro_rules! impl_decodable {
    ($($typ:ident,)+) => {
        $(impl $crate::common::encodable::Decodable for $typ {
            type Error = PacketError<Self>;
            type Cond = Option<FixedHeader>;

            fn decode_with<R: std::io::Read>(
                reader: &mut R,
                fixed_header: Self::Cond,
            ) -> Result<Self, Self::Error> {
                let fixed_header: FixedHeader = if let Some(hdr) = fixed_header {
                    hdr
                } else {
                    $crate::common::encodable::Decodable::decode(reader)?
                };

                <Self as DecodablePacket>::decode_packet(reader, fixed_header)
            }
        })+
    };
}

impl_decodable! {
    ConnectPacket,
    ConnackPacket,

    PublishPacket,
    PubackPacket,
    PubrecPacket,
    PubrelPacket,
    PubcompPacket,

    PingreqPacket,
    PingrespPacket,

    SubscribePacket,
    SubackPacket,

    UnsubscribePacket,
    UnsubackPacket,

    DisconnectPacket,
    AuthPacket,
}

/// Parsing errors for packet
#[derive(thiserror::Error)]
#[error(transparent)]
pub enum PacketError<P>
where
    P: DecodablePacket,
{
    FixedHeaderError(#[from] FixedHeaderError),
    VariableHeaderError(#[from] VariableHeaderError),
    PayloadError(<P as DecodablePacket>::DecodePacketError),
    IoError(#[from] io::Error),
    TopicNameError(#[from] TopicNameError),
}

impl<P> Debug for PacketError<P>
where
    P: DecodablePacket,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            PacketError::FixedHeaderError(ref e) => {
                f.debug_tuple("FixedHeaderError").field(e).finish()
            }
            PacketError::VariableHeaderError(ref e) => {
                f.debug_tuple("VariableHeaderError").field(e).finish()
            }
            PacketError::PayloadError(ref e) => f.debug_tuple("PayloadError").field(e).finish(),
            PacketError::IoError(ref e) => f.debug_tuple("IoError").field(e).finish(),
            PacketError::TopicNameError(ref e) => f.debug_tuple("TopicNameError").field(e).finish(),
        }
    }
}

impl<P: DecodablePacket> From<TopicNameDecodeError> for PacketError<P> {
    fn from(e: TopicNameDecodeError) -> Self {
        match e {
            TopicNameDecodeError::IoError(e) => e.into(),
            TopicNameDecodeError::InvalidTopicName(e) => e.into(),
        }
    }
}

macro_rules! impl_variable_packet {
    ($($name:ident & $errname:ident => $hdr:ident,)+) => {
        /// Variable packet
        #[derive(Debug, Eq, PartialEq, Clone)]
        pub enum VariablePacket {
            $(
                $name($name),
            )+
        }

        #[cfg(all(feature = "v5", feature = "parse"))]
        impl VariablePacket {
            /// Asynchronously parse a packet from a `tokio::io::AsyncRead`
            pub async fn parse<A: AsyncRead + Unpin>(rdr: &mut A) -> Result<Self, VariablePacketError> {
                use std::io::Cursor;
                let fixed_header = FixedHeader::parse(rdr).await?;

                let mut buffer = vec![0u8; fixed_header.remaining_length as usize];
                rdr.read_exact(&mut buffer).await?;

                decode_with_header(&mut Cursor::new(buffer), fixed_header)
            }
        }

        #[inline]
        fn decode_with_header<R: io::Read>(rdr: &mut R, fixed_header: FixedHeader) -> Result<VariablePacket, VariablePacketError> {
            match fixed_header.packet_type.control_type() {
                $(
                    ControlType::$hdr => {
                        let pk = <$name as DecodablePacket>::decode_packet(rdr, fixed_header)?;
                        Ok(VariablePacket::$name(pk))
                    }
                )+
            }
        }

        $(
            impl From<$name> for VariablePacket {
                fn from(pk: $name) -> VariablePacket {
                    VariablePacket::$name(pk)
                }
            }
        )+

        // impl Encodable for VariablePacket {
        //     fn encode<W: Write>(&self, writer: &mut W) -> Result<(), io::Error> {
        //         match *self {
        //             $(
        //                 VariablePacket::$name(ref pk) => pk.encode(writer),
        //             )+
        //         }
        //     }

        //     fn encoded_length(&self) -> u32 {
        //         match *self {
        //             $(
        //                 VariablePacket::$name(ref pk) => pk.encoded_length(),
        //             )+
        //         }
        //     }
        // }

        impl EncodablePacket for VariablePacket {
            type Output = FixedHeader;

            fn fixed_header(&self) -> &Self::Output {
                match *self {
                    $(
                        VariablePacket::$name(ref pk) => pk.fixed_header(),
                    )+
                }
            }

            fn encode_packet<W: Write>(&self, writer: &mut W) -> io::Result<()> {
                match *self {
                    $(
                        VariablePacket::$name(ref pk) => pk.encode_packet(writer),
                    )+
                }
            }

            fn encoded_packet_length(&self) -> u32 {
                match *self {
                    $(
                        VariablePacket::$name(ref pk) => pk.encoded_packet_length(),
                    )+
                }
            }
        }

        impl Decodable for VariablePacket {
            type Error = VariablePacketError;
            type Cond = Option<FixedHeader>;

            fn decode_with<R: Read>(reader: &mut R, fixed_header: Self::Cond)
                    -> Result<VariablePacket, Self::Error> {
                let fixed_header = match fixed_header {
                    Some(fh) => fh,
                    None => {
                        match FixedHeader::decode(reader) {
                            Ok(header) => header,
                            Err(FixedHeaderError::ReservedType(code, length)) => {
                                let reader = &mut reader.take(length as u64);
                                let mut buf = Vec::with_capacity(length as usize);
                                reader.read_to_end(&mut buf)?;
                                return Err(VariablePacketError::ReservedPacket(code, buf));
                            },
                            Err(err) => return Err(From::from(err))
                        }
                    }
                };
                let reader = &mut reader.take(fixed_header.remaining_length as u64);

                decode_with_header(reader, fixed_header)
            }
        }

        /// Parsing errors for variable packet
        #[derive(Debug, thiserror::Error)]
        pub enum VariablePacketError {
            #[error(transparent)]
            FixedHeaderError(#[from] FixedHeaderError),
            #[error("reserved packet type ({0}), [u8, ..{}]", .1.len())]
            ReservedPacket(u8, Vec<u8>),
            #[error(transparent)]
            IoError(#[from] io::Error),
            $(
                #[error(transparent)]
                $errname(#[from] PacketError<$name>),
            )+
        }
    }
}

impl_variable_packet! {
    ConnectPacket       & ConnectPacketError        => Connect,
    ConnackPacket       & ConnackPacketError        => ConnectAcknowledgement,

    PublishPacket       & PublishPacketError        => Publish,
    PubackPacket        & PubackPacketError         => PublishAcknowledgement,
    PubrecPacket        & PubrecPacketError         => PublishReceived,
    PubrelPacket        & PubrelPacketError         => PublishRelease,
    PubcompPacket       & PubcompPacketError        => PublishComplete,

    PingreqPacket       & PingreqPacketError        => PingRequest,
    PingrespPacket      & PingrespPacketError       => PingResponse,

    SubscribePacket     & SubscribePacketError      => Subscribe,
    SubackPacket        & SubackPacketError         => SubscribeAcknowledgement,

    UnsubscribePacket   & UnsubscribePacketError    => Unsubscribe,
    UnsubackPacket      & UnsubackPacketError       => UnsubscribeAcknowledgement,

    DisconnectPacket    & DisconnectPacketError     => Disconnect,
    AuthPacket          & AuthPacketError           => Auth,
}

impl VariablePacket {
    pub fn new<T>(t: T) -> VariablePacket
    where
        VariablePacket: From<T>,
    {
        From::from(t)
    }
}

#[cfg(feature = "tokio-codec")]
mod codec {
    use bytes::{Buf as _, BufMut as _, BytesMut};
    use tokio_util::codec;

    use super::*;
    use crate::{
        common::{packet::EncodablePacket, Encodable},
        v5::control::packet_type::{PacketType, PacketTypeError},
    };

    pub struct MqttDecoder {
        state: DecodeState,
    }

    enum DecodeState {
        Start,
        Packet { length: u32, typ: DecodePacketType },
    }

    #[derive(Copy, Clone)]
    enum DecodePacketType {
        Standard(PacketType),
        Reserved(u8),
    }

    impl MqttDecoder {
        pub const fn new() -> Self {
            MqttDecoder {
                state: DecodeState::Start,
            }
        }
    }

    impl Default for MqttDecoder {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Like FixedHeader::decode(), but on a buffer instead of a stream. Returns None if it reaches
    /// the end of the buffer before it finishes decoding the header.
    #[inline]
    fn decode_header(
        mut data: &[u8],
    ) -> Option<Result<(DecodePacketType, u32, usize), FixedHeaderError>> {
        let mut header_size = 0;
        macro_rules! read_u8 {
            () => {{
                let (&x, rest) = data.split_first()?;
                data = rest;
                header_size += 1;
                x
            }};
        }

        let type_val = read_u8!();
        let remaining_len = {
            let mut cur = 0u32;
            for i in 0.. {
                let byte = read_u8!();
                cur |= ((byte as u32) & 0x7F) << (7 * i);

                if i >= 4 {
                    return Some(Err(FixedHeaderError::MalformedRemainingLength));
                }

                if byte & 0x80 == 0 {
                    break;
                }
            }

            cur
        };

        let packet_type = match PacketType::try_from(type_val) {
            Ok(ty) => DecodePacketType::Standard(ty),
            Err(PacketTypeError::ReservedType(ty, _)) => DecodePacketType::Reserved(ty),
            Err(err) => return Some(Err(err.into())),
        };
        Some(Ok((packet_type, remaining_len, header_size)))
    }

    impl codec::Decoder for MqttDecoder {
        type Item = VariablePacket;
        type Error = VariablePacketError;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            loop {
                match &mut self.state {
                    DecodeState::Start => match decode_header(&src[..]) {
                        Some(Ok((typ, length, header_size))) => {
                            src.advance(header_size);
                            self.state = DecodeState::Packet { length, typ };
                            continue;
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(None),
                    },
                    DecodeState::Packet { length, typ } => {
                        let length = *length;
                        if src.remaining() < length as usize {
                            return Ok(None);
                        }
                        let typ = *typ;

                        self.state = DecodeState::Start;

                        match typ {
                            DecodePacketType::Standard(typ) => {
                                let header = FixedHeader {
                                    packet_type: typ,
                                    remaining_length: length,
                                };
                                return decode_with_header(&mut src.reader(), header).map(Some);
                            }
                            DecodePacketType::Reserved(code) => {
                                let data = src[..length as usize].to_vec();
                                src.advance(length as usize);
                                return Err(VariablePacketError::ReservedPacket(code, data));
                            }
                        }
                    }
                }
            }
        }
    }

    pub struct MqttEncoder {}

    impl MqttEncoder {
        pub const fn new() -> Self {
            MqttEncoder {}
        }
    }

    impl Default for MqttEncoder {
        fn default() -> Self {
            Self::new()
        }
    }

    impl<T: EncodablePacket + Encodable> codec::Encoder<T> for MqttEncoder {
        type Error = io::Error;
        fn encode(&mut self, packet: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
            dst.reserve(packet.encoded_length() as usize);
            packet.encode(&mut dst.writer())
        }
    }

    pub struct MqttCodec {
        decode: MqttDecoder,
        encode: MqttEncoder,
    }

    impl MqttCodec {
        pub const fn new() -> Self {
            MqttCodec {
                decode: MqttDecoder::new(),
                encode: MqttEncoder::new(),
            }
        }
    }

    impl Default for MqttCodec {
        fn default() -> Self {
            Self::new()
        }
    }

    impl codec::Decoder for MqttCodec {
        type Item = VariablePacket;
        type Error = VariablePacketError;
        #[inline]
        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            self.decode.decode(src)
        }
    }

    impl<T: EncodablePacket + Encodable> codec::Encoder<T> for MqttCodec {
        type Error = io::Error;
        #[inline]
        fn encode(&mut self, packet: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
            self.encode.encode(packet, dst)
        }
    }
}

#[cfg(feature = "tokio-codec")]
pub use codec::{MqttCodec, MqttDecoder, MqttEncoder};

#[cfg(test)]
mod test {
    use subscribe::{RetainHandling, SubscribeOptions};

    use super::*;

    use std::io::Cursor;

    use crate::common::{Decodable, Encodable};

    #[test]
    fn test_variable_packet_basic() {
        let packet = ConnectPacket::new("1234".to_owned());

        // Wrap it
        let var_packet = VariablePacket::new(packet);

        // Encode
        let mut buf = Vec::new();
        var_packet.encode(&mut buf).unwrap();

        // Decode
        let mut decode_buf = Cursor::new(buf);
        let decoded_packet = VariablePacket::decode(&mut decode_buf).unwrap();

        assert_eq!(var_packet, decoded_packet);
    }

    #[cfg(all(feature = "v5", feature = "parse"))]
    #[tokio::test]
    async fn test_variable_packet_async_parse() {
        let packet = ConnectPacket::new("1234".to_owned());

        // Wrap it
        let var_packet = VariablePacket::new(packet);

        // Encode
        let mut buf = Vec::new();
        var_packet.encode(&mut buf).unwrap();

        // Parse
        let mut async_buf = buf.as_slice();
        let decoded_packet = VariablePacket::parse(&mut async_buf).await.unwrap();

        assert_eq!(var_packet, decoded_packet);
    }

    #[cfg(feature = "tokio-codec")]
    #[tokio::test]
    async fn test_variable_packet_framed() {
        use crate::common::{QualityOfService, TopicFilter};
        use futures::{SinkExt, StreamExt};
        use tokio_util::codec::{FramedRead, FramedWrite};

        let conn_packet = ConnectPacket::new("1234".to_owned());
        let mut subscribe_options = SubscribeOptions::default();
        subscribe_options.set_qos(QualityOfService::Level0);
        subscribe_options.set_no_local(false);
        subscribe_options.set_retain_as_published(false);
        subscribe_options.set_retain_handling(RetainHandling::SendAtSubscribe);

        let sub_packet = SubscribePacket::new(
            1,
            vec![(TopicFilter::new("foo/#").unwrap(), subscribe_options)],
        );

        // small, to make sure buffering and stuff works
        let (reader, writer) = tokio::io::duplex(8);

        let task = tokio::spawn({
            let (conn_packet, sub_packet) = (conn_packet.clone(), sub_packet.clone());
            async move {
                let mut sink = FramedWrite::new(writer, MqttEncoder::new());
                sink.send(conn_packet).await.unwrap();
                sink.send(sub_packet).await.unwrap();
                SinkExt::<VariablePacket>::flush(&mut sink).await.unwrap();
            }
        });

        let mut stream = FramedRead::new(reader, MqttDecoder::new());
        let decoded_conn = stream.next().await.unwrap().unwrap();
        let decoded_sub = stream.next().await.unwrap().unwrap();

        task.await.unwrap();

        assert!(stream.next().await.is_none());

        assert_eq!(decoded_conn, conn_packet.into());
        assert_eq!(decoded_sub, sub_packet.into());
    }
}
