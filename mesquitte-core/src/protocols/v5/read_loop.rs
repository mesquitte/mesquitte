use futures::StreamExt as _;
use kanal::AsyncSender;
use mqtt_codec_kit::v5::packet::{VariablePacket, VariablePacketError};
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, FramedRead};

use crate::{error, warn};

pub(crate) struct ReadLoop<T, D> {
    reader: FramedRead<T, D>,
    read_tx: AsyncSender<VariablePacket>,
}

impl<T, D> ReadLoop<T, D>
where
    T: AsyncRead + Unpin,
    D: Decoder<Item = VariablePacket, Error = VariablePacketError>,
{
    pub fn new(reader: FramedRead<T, D>, read_tx: AsyncSender<VariablePacket>) -> Self {
        Self { reader, read_tx }
    }

    pub async fn read_from_client(mut self) {
        loop {
            match self.reader.next().await {
                Some(Ok(pkt)) => match self.read_tx.send(pkt).await {
                    Ok(_) => {}
                    Err(err) => {
                        error!("send to write: {}", err);
                        break;
                    }
                },
                Some(Err(err)) => {
                    error!("read packet: {}", err);
                    break;
                }
                None => {
                    warn!("reader is closed");
                    break;
                }
            }
        }
    }
}
