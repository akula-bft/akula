use crate::downloader::messages::*;

pub fn decode_rlp_message(id: EthMessageId, message_bytes: &[u8]) -> anyhow::Result<Message> {
    let message: Message = match id {
        EthMessageId::GetBlockHeaders => {
            Message::GetBlockHeaders(rlp::decode::<GetBlockHeadersMessage>(message_bytes)?)
        }
        EthMessageId::NewBlockHashes => {
            Message::NewBlockHashes(rlp::decode::<NewBlockHashesMessage>(message_bytes)?)
        }
        _ => anyhow::bail!("decode_rlp_message: unsupported message {:?}", id),
    };
    Ok(message)
}

impl rlp::Encodable for Message {
    fn rlp_append(&self, stream: &mut rlp::RlpStream) {
        match self {
            Message::GetBlockHeaders(message) => message.rlp_append(stream),
            Message::NewBlockHashes(message) => message.rlp_append(stream),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::downloader::{
        message_decoder::decode_rlp_message,
        messages::{EthMessageId, Message},
    };
    use ethereum_types::H256;
    use hex_literal::hex;

    #[test]
    fn decode_new_block_hashes() {
        let expected_bytes =
            hex!("e6e5a07100614faba6650b53fe0913ed7267bcc968eb362e3df908645a50aa526c72ba83a13ead");
        let result = decode_rlp_message(EthMessageId::NewBlockHashes, &expected_bytes);
        let some_message = result.unwrap();

        let bytes = rlp::encode(&some_message);
        assert_eq!(&*bytes, expected_bytes);

        if let Message::NewBlockHashes(message) = some_message {
            assert_eq!(message.ids.len(), 1);
            assert_eq!(
                message.ids[0].0,
                H256(hex!(
                    "7100614faba6650b53fe0913ed7267bcc968eb362e3df908645a50aa526c72ba"
                ))
            );
            assert_eq!(message.ids[0].1, 10567341);
        } else {
            assert!(false, "unexpected message type");
        }
    }
}
