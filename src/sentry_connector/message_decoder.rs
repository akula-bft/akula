use super::messages::*;
use crate::sentry_connector::messages::Message;
use fastrlp::*;

pub fn decode_rlp_message(id: EthMessageId, mut message_bytes: &[u8]) -> anyhow::Result<Message> {
    let message_bytes = &mut message_bytes;
    let message: Message = match id {
        EthMessageId::Status => Message::Status(Decodable::decode(message_bytes)?),
        EthMessageId::NewBlockHashes => Message::NewBlockHashes(Decodable::decode(message_bytes)?),
        EthMessageId::Transactions => Message::Transactions(Decodable::decode(message_bytes)?),
        EthMessageId::GetBlockHeaders => {
            Message::GetBlockHeaders(Decodable::decode(message_bytes)?)
        }
        EthMessageId::BlockHeaders => Message::BlockHeaders(Decodable::decode(message_bytes)?),
        EthMessageId::GetBlockBodies => Message::GetBlockBodies(Decodable::decode(message_bytes)?),
        EthMessageId::BlockBodies => Message::BlockBodies(Decodable::decode(message_bytes)?),
        EthMessageId::NewBlock => Message::NewBlock(Decodable::decode(message_bytes)?),
        EthMessageId::NewPooledTransactionHashes => {
            Message::NewPooledTransactionHashes(Decodable::decode(message_bytes)?)
        }
        EthMessageId::GetPooledTransactions => {
            Message::GetPooledTransactions(Decodable::decode(message_bytes)?)
        }
        EthMessageId::PooledTransactions => {
            Message::PooledTransactions(Decodable::decode(message_bytes)?)
        }
        EthMessageId::GetNodeData => Message::GetNodeData(Decodable::decode(message_bytes)?),
        EthMessageId::NodeData => Message::NodeData(Decodable::decode(message_bytes)?),
        EthMessageId::GetReceipts => Message::GetReceipts(Decodable::decode(message_bytes)?),
        EthMessageId::Receipts => Message::Receipts(Decodable::decode(message_bytes)?),
    };
    Ok(message)
}

impl Encodable for Message {
    fn encode(&self, out: &mut dyn BufMut) {
        match self {
            Message::Status(v) => v.encode(out),
            Message::NewBlockHashes(v) => v.encode(out),
            Message::Transactions(v) => v.encode(out),
            Message::GetBlockHeaders(v) => v.encode(out),
            Message::BlockHeaders(v) => v.encode(out),
            Message::GetBlockBodies(v) => v.encode(out),
            Message::BlockBodies(v) => v.encode(out),
            Message::NewBlock(v) => v.encode(out),
            Message::NewPooledTransactionHashes(v) => v.encode(out),
            Message::GetPooledTransactions(v) => v.encode(out),
            Message::PooledTransactions(v) => v.encode(out),
            Message::GetNodeData(v) => v.encode(out),
            Message::NodeData(v) => v.encode(out),
            Message::GetReceipts(v) => v.encode(out),
            Message::Receipts(v) => v.encode(out),
        }
    }
    fn length(&self) -> usize {
        match self {
            Message::Status(v) => v.length(),
            Message::NewBlockHashes(v) => v.length(),
            Message::Transactions(v) => v.length(),
            Message::GetBlockHeaders(v) => v.length(),
            Message::BlockHeaders(v) => v.length(),
            Message::GetBlockBodies(v) => v.length(),
            Message::BlockBodies(v) => v.length(),
            Message::NewBlock(v) => v.length(),
            Message::NewPooledTransactionHashes(v) => v.length(),
            Message::GetPooledTransactions(v) => v.length(),
            Message::PooledTransactions(v) => v.length(),
            Message::GetNodeData(v) => v.length(),
            Message::NodeData(v) => v.length(),
            Message::GetReceipts(v) => v.length(),
            Message::Receipts(v) => v.length(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{block_id::BlockId, message_decoder::decode_rlp_message, messages::*};
    use crate::{
        models::{
            BlockHeader, BlockNumber, ChainId, Message as TxMessage, MessageSignature,
            MessageWithSignature, Receipt, TransactionAction,
        },
        sentry_connector::messages::Message,
    };
    use bytes::{Bytes, BytesMut};
    use ethereum_types::{Bloom, H160, H256, H64};
    use ethnum::U256;
    use fastrlp::*;
    use hex_literal::hex;

    #[test]
    fn decode_new_block_hashes() {
        let expected_bytes =
            hex!("e6e5a07100614faba6650b53fe0913ed7267bcc968eb362e3df908645a50aa526c72ba83a13ead");
        let result = decode_rlp_message(EthMessageId::NewBlockHashes, &expected_bytes);
        let some_message = result.unwrap();

        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::NewBlockHashes(NewBlockHashesMessage(vec![BlockHashAndNumber {
                hash: H256(hex!(
                    "7100614faba6650b53fe0913ed7267bcc968eb362e3df908645a50aa526c72ba"
                )),
                number: BlockNumber(10567341),
            },],))
        );
    }

    #[test]
    fn decode_get_block_headers() {
        let expected_bytes = hex!("ca820457c682270f050580");
        let some_message =
            decode_rlp_message(EthMessageId::GetBlockHeaders, &expected_bytes).unwrap();

        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetBlockHeaders(GetBlockHeadersMessage {
                request_id: 0x0457,
                params: GetBlockHeadersMessageParams {
                    start_block: BlockId::Number(0x270f.into()),
                    limit: 0x05,
                    skip: 0x05,
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

        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
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

    #[test]
    /// Test case taken from https://eips.ethereum.org/EIPS/eip-2481
    fn decode_get_block_bodies() {
        let expected_bytes =
            hex!("f847820457f842a000000000000000000000000000000000000000000000000000000000deadc0dea000000000000000000000000000000000000000000000000000000000feedbeef");
        let result = decode_rlp_message(EthMessageId::GetBlockBodies, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetBlockBodies(GetBlockBodiesMessage {
                request_id: 1111,
                block_hashes: vec![
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000deadc0de"
                    )),
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000feedbeef"
                    ))
                ]
            })
        );
    }
    #[test]
    /// Test case taken from https://eips.ethereum.org/EIPS/eip-2481
    fn decode_block_headers() {
        let expected_bytes =
            hex!("f90202820457f901fcf901f9a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008208ae820d0582115c8215b3821a0a827788a00000000000000000000000000000000000000000000000000000000000000000880000000000000000");
        let result = decode_rlp_message(EthMessageId::BlockHeaders, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::BlockHeaders(BlockHeadersMessage {
                request_id: 1111,
                headers: vec! [
                    BlockHeader{
                        parent_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        ommers_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        beneficiary: H160(hex!("0000000000000000000000000000000000000000")),
                        state_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        transactions_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        receipts_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        logs_bloom: Bloom(hex!("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
                        difficulty: U256::from_str_radix("8ae", 16).unwrap(),
                        number: BlockNumber(0xd05),
                        gas_limit: 0x115c,
                        gas_used: 0x15b3,
                        timestamp: 0x1a0a,
                        extra_data: Bytes::from(vec![0x77, 0x88]),
                        mix_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        nonce: H64(hex!("0000000000000000")),
                        base_fee_per_gas: None,
                    }
                ]
            })
        );
    }
    #[test]
    /// Test case taken from https://eips.ethereum.org/EIPS/eip-2481
    fn decode_block_bodies() {
        let expected_bytes =
            hex!("f902dc820457f902d6f902d3f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afbf901fcf901f9a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000940000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000a00000000000000000000000000000000000000000000000000000000000000000b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008208ae820d0582115c8215b3821a0a827788a00000000000000000000000000000000000000000000000000000000000000000880000000000000000");
        let result = decode_rlp_message(EthMessageId::BlockBodies, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
        Message::BlockBodies(BlockBodiesMessage {
                request_id: 1111,
                block_bodies: vec![BlockBodyType {
                    transactions: vec![
                        MessageWithSignature {
                            message: TxMessage::Legacy {
                                chain_id: Some(ChainId(1)),
                                nonce: 0x8,
                                gas_price: U256::from_str_radix("4a817c808", 16).unwrap(),
                                gas_limit: 0x2e248,
                                action: TransactionAction::Call(H160(hex!(
                                    "3535353535353535353535353535353535353535"
                                ))),
                                value: U256::from_str_radix("200", 16).unwrap(),
                                input: Bytes::new(),
                            },
                            signature: MessageSignature::new(
                                false,
                                H256(hex!(
                                "64b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12"
                                )),
                                    H256(hex!(
                                    "64b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"
                                )),
                            ).unwrap()
                        },
                        MessageWithSignature{
                            message: TxMessage::Legacy {
                                chain_id: Some(ChainId(1)),
                                nonce: 0x9,
                                gas_price: U256::from_str_radix("4a817c809", 16).unwrap(),
                                gas_limit: 0x33450,
                                action: TransactionAction::Call(H160(hex!(
                                    "3535353535353535353535353535353535353535"
                                ))),
                                value: U256::from_str_radix("2d9", 16).unwrap(),
                                input: Bytes::new(),
                            },
                            signature: MessageSignature::new(
                                false,
                                H256(hex!(
                                "52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"
                                )),
                                    H256(hex!(
                                    "52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"
                                )),
                            ).unwrap()
                        }
                    ],
                    ommers: vec![ BlockHeader{
                        parent_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        ommers_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        beneficiary: H160(hex!("0000000000000000000000000000000000000000")),
                        state_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        transactions_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        receipts_root: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        logs_bloom: Bloom(hex!("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
                        difficulty: U256::from_str_radix("8ae", 16).unwrap(),
                        number: BlockNumber(0xd05),
                        gas_limit: 0x115c,
                        gas_used: 0x15b3,
                        timestamp: 0x1a0a,
                        extra_data: vec![0x77, 0x88].into(),
                        mix_hash: H256(hex!("0000000000000000000000000000000000000000000000000000000000000000")),
                        nonce: H64(hex!("0000000000000000")),
                        base_fee_per_gas: None
                    }]
                }]
            })
        );
    }

    #[test]
    /// Test case taken from https://eips.ethereum.org/EIPS/eip-2481
    fn decode_get_node_data() {
        let expected_bytes =
            hex!("f847820457f842a000000000000000000000000000000000000000000000000000000000deadc0dea000000000000000000000000000000000000000000000000000000000feedbeef");
        let result = decode_rlp_message(EthMessageId::GetNodeData, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetNodeData(GetNodeDataMessage {
                request_id: 1111,
                hashes: vec![
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000deadc0de"
                    )),
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000feedbeef"
                    ))
                ]
            })
        );
    }

    #[test]
    fn decode_get_receipts() {
        let expected_bytes =
        hex!("f847820457f842a000000000000000000000000000000000000000000000000000000000deadc0dea000000000000000000000000000000000000000000000000000000000feedbeef");
        let result = decode_rlp_message(EthMessageId::GetReceipts, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::GetReceipts(GetReceiptsMessage {
                request_id: 1111,
                block_hashes: vec![
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000deadc0de"
                    )),
                    H256(hex!(
                        "00000000000000000000000000000000000000000000000000000000feedbeef"
                    ))
                ]
            })
        );
    }

    #[test]
    fn decode_receipts() {
        let expected_bytes =
        hex!("f90172820457f9016cf90169f901668001b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f85ff85d940000000000000000000000000000000000000011f842a0000000000000000000000000000000000000000000000000000000000000deada0000000000000000000000000000000000000000000000000000000000000beef830100ff");
        let result = decode_rlp_message(EthMessageId::Receipts, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        assert_eq!(
            some_message,
            Message::Receipts(ReceiptsMessage {
                request_id: 0x0457,
                receipts: vec![{
                    BlockReceipts (
                        vec![Receipt {
                            logs: vec![crate::models::Log {
                                address: H160(hex!("0000000000000000000000000000000000000011")),
                                topics: vec![
                                    H256(hex!(
                                        "000000000000000000000000000000000000000000000000000000000000dead"
                                    )),
                                    H256(hex!(
                                        "000000000000000000000000000000000000000000000000000000000000beef"
                                    )),
                                ],
                                data: vec![0x01, 0x00, 0xff].into(),
                            }],
                            success: false,
                            cumulative_gas_used: 0x01,
                            bloom: Bloom(hex!("00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
                            tx_type: crate::models::TxType::Legacy,
                        }],
                    )
                }]
            })
        );
    }

    #[test]
    fn decode_node_data() {
        let expected_bytes = hex!("ce820457ca84deadc0de84feedbeef");
        let result = decode_rlp_message(EthMessageId::NodeData, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        let msg = Message::NodeData(NodeDataMessage {
            request_id: 0x0457,
            data: vec![
                hex!("deadc0de").to_vec().into(),
                hex!("feedbeef").to_vec().into(),
            ],
        });

        assert_eq!(some_message, msg);
    }

    #[test]
    fn decode_get_pooled_transactions() {
        let expected_bytes = hex!("f847820457f842a000000000000000000000000000000000000000000000000000000000deadc0dea000000000000000000000000000000000000000000000000000000000feedbeef");
        let result = decode_rlp_message(EthMessageId::GetPooledTransactions, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        let msg = Message::GetPooledTransactions(GetPooledTransactionsMessage {
            request_id: 1111,
            tx_hashes: vec![
                H256(hex!(
                    "00000000000000000000000000000000000000000000000000000000deadc0de"
                )),
                H256(hex!(
                    "00000000000000000000000000000000000000000000000000000000feedbeef"
                )),
            ],
        });

        assert_eq!(some_message, msg);
    }

    #[test]
    fn decode_pooled_transactions() {
        let expected_bytes = hex!("f8d7820457f8d2f867088504a817c8088302e2489435353535353535353535353535353535353535358202008025a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12a064b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10f867098504a817c809830334509435353535353535353535353535353535353535358202d98025a052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afba052f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb");
        let result = decode_rlp_message(EthMessageId::PooledTransactions, &expected_bytes);
        let some_message = result.unwrap();
        let mut bytes = BytesMut::new();
        some_message.encode(&mut bytes);
        assert_eq!(&*bytes, expected_bytes);

        let msg = Message::PooledTransactions(PooledTransactionsMessage {
            request_id: 1111,
            transactions: vec![
                MessageWithSignature {
                    message: TxMessage::Legacy {
                        chain_id: Some(ChainId(1)),
                        nonce: 0x8,
                        gas_price: U256::from_str_radix("4a817c808", 16).unwrap(),
                        gas_limit: 0x2e248,
                        action: TransactionAction::Call(H160(hex!(
                            "3535353535353535353535353535353535353535"
                        ))),
                        value: U256::from_str_radix("200", 16).unwrap(),
                        input: vec![].into(),
                    },
                    signature: MessageSignature::new(
                        false,
                        H256(hex!(
                            "64b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c12"
                        )),
                        H256(hex!(
                            "64b1702d9298fee62dfeccc57d322a463ad55ca201256d01f62b45b2e1c21c10"
                        )),
                    )
                    .unwrap(),
                },
                MessageWithSignature {
                    message: TxMessage::Legacy {
                        chain_id: Some(ChainId(1)),
                        nonce: 0x9,
                        gas_price: U256::from_str_radix("4a817c809", 16).unwrap(),
                        gas_limit: 0x33450,
                        action: TransactionAction::Call(H160(hex!(
                            "3535353535353535353535353535353535353535"
                        ))),
                        value: U256::from_str_radix("2d9", 16).unwrap(),
                        input: vec![].into(),
                    },
                    signature: MessageSignature::new(
                        false,
                        H256(hex!(
                            "52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"
                        )),
                        H256(hex!(
                            "52f8f61201b2b11a78d6e866abc9c3db2ae8631fa656bfe5cb53668255367afb"
                        )),
                    )
                    .unwrap(),
                },
            ],
        });

        assert_eq!(some_message, msg);
    }
}
