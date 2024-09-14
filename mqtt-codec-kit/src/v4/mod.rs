//! MQTT 3.1.1 protocol utilities library
//!
//! Strictly implements protocol of [MQTT v3.1.1](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
//!
//! ## Usage
//!
//! ```rust
//! use std::io::Cursor;
//!
//! use mqtt_codec_kit::common::{qos::QoSWithPacketIdentifier, Decodable, Encodable, TopicName};
//! use mqtt_codec_kit::v4::packet::{PublishPacket, VariablePacket};
//!
//! // Create a new Publish packet
//! let packet = PublishPacket::new(TopicName::new("mqtt/learning").unwrap(),
//!                                 QoSWithPacketIdentifier::Level2(10),
//!                                 b"Hello MQTT!".to_vec());
//!
//! // Encode
//! let mut buf = Vec::new();
//! packet.encode(&mut buf).unwrap();
//! println!("Encoded: {:?}", buf);
//!
//! // Decode it with known type
//! let mut dec_buf = Cursor::new(&buf[..]);
//! let decoded = PublishPacket::decode(&mut dec_buf).unwrap();
//! println!("Decoded: {:?}", decoded);
//! assert_eq!(packet, decoded);
//!
//! // Auto decode by the fixed header
//! let mut dec_buf = Cursor::new(&buf[..]);
//! let auto_decode = VariablePacket::decode(&mut dec_buf).unwrap();
//! println!("Variable packet decode: {:?}", auto_decode);
//! assert_eq!(VariablePacket::PublishPacket(packet), auto_decode);
//! ```

pub mod control;
pub mod packet;
