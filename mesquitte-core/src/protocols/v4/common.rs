use mqtt_codec_kit::v4::packet::{DisconnectPacket, VariablePacket};

#[derive(Debug)]
pub enum WritePacket {
    Stop,
    Packet(VariablePacket),
    Packets(Vec<VariablePacket>),
    Disconnect(DisconnectPacket),
}
