use crate::downloader::messages::*;

pub fn decode_rlp_message(id: EthMessageId, message_bytes: &[u8]) -> anyhow::Result<Message> {
    let message: Message = match id {
        EthMessageId::GetBlockHeaders => {
            Message::GetBlockHeaders(rlp::decode::<GetBlockHeadersMessage>(message_bytes)?)
        }
        _ => anyhow::bail!("decode_rlp_message: unsupported message {:?}", id),
    };
    Ok(message)
}

impl rlp::Encodable for Message {
    fn rlp_append(&self, stream: &mut rlp::RlpStream) {
        match self {
            Message::GetBlockHeaders(message) => message.rlp_append(stream),
        }
    }
}
