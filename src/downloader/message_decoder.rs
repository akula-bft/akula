use crate::downloader::messages::*;

pub fn decode_rlp_message(id: EthMessageId, message_bytes: &[u8]) -> anyhow::Result<Message> {
    let message: Message = match id {
        EthMessageId::NewBlockHashes => {
            Message::NewBlockHashes(rlp::decode::<NewBlockHashesMessage>(message_bytes)?)
        }
        EthMessageId::GetBlockHeaders => {
            Message::GetBlockHeaders(rlp::decode::<GetBlockHeadersMessage>(message_bytes)?)
        }
        EthMessageId::BlockHeaders => {
            Message::BlockHeaders(rlp::decode::<BlockHeadersMessage>(message_bytes)?)
        }
        EthMessageId::NewBlock => Message::NewBlock(rlp::decode::<NewBlockMessage>(message_bytes)?),
        EthMessageId::NewPooledTransactionHashes => Message::NewPooledTransactionHashes(
            rlp::decode::<NewPooledTransactionHashesMessage>(message_bytes)?,
        ),
        _ => anyhow::bail!("decode_rlp_message: unsupported message {:?}", id),
    };
    Ok(message)
}

impl rlp::Encodable for Message {
    fn rlp_append(&self, stream: &mut rlp::RlpStream) {
        match self {
            Message::NewBlockHashes(message) => message.rlp_append(stream),
            Message::GetBlockHeaders(message) => message.rlp_append(stream),
            Message::BlockHeaders(message) => message.rlp_append(stream),
            Message::NewBlock(message) => message.rlp_append(stream),
            Message::NewPooledTransactionHashes(message) => message.rlp_append(stream),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::downloader::{
        block_id::BlockId,
        message_decoder::decode_rlp_message,
        messages::{
            BlockHashAndNumber, EthMessageId, GetBlockHeadersMessage, GetBlockHeadersMessageParams,
            Message, NewBlockHashesMessage,
        },
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

        assert_eq!(
            some_message,
            Message::NewBlockHashes(NewBlockHashesMessage {
                ids: vec![BlockHashAndNumber {
                    hash: H256(hex!(
                        "7100614faba6650b53fe0913ed7267bcc968eb362e3df908645a50aa526c72ba"
                    )),
                    number: 10567341,
                },],
            })
        );
    }

    #[test]
    fn decode_get_block_headers() {
        let expected_bytes = hex!("ca820457c682270f050580");
        let result = decode_rlp_message(EthMessageId::GetBlockHeaders, &expected_bytes);
        let some_message = result.unwrap();

        let bytes = rlp::encode(&some_message);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetBlockHeaders(GetBlockHeadersMessage {
                request_id: 1111,
                params: GetBlockHeadersMessageParams {
                    start_block: BlockId::Number(9999),
                    limit: 5,
                    skip: 5,
                    reverse: 0,
                },
            })
        );
    }

    #[test]
    fn decode_get_block_headers_hash() {
        let expected_bytes = hex!(
            "e8820457e4a000000000000000000000000000000000000000000000000000000000deadc0de050601"
        );
        let result = decode_rlp_message(EthMessageId::GetBlockHeaders, &expected_bytes);
        let some_message = result.unwrap();

        let bytes = rlp::encode(&some_message);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetBlockHeaders(GetBlockHeadersMessage {
                request_id: 1111,
                params: GetBlockHeadersMessageParams {
                    start_block: BlockId::Hash(H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000deadc0de"
                    ))),
                    limit: 5,
                    skip: 6,
                    reverse: 1,
                },
            })
        );
    }
}
