use super::block_id::BlockId;
use crate::models::{
    Block as BlockType, BlockHeader as HeaderType, MessageWithSignature, Receipt as ReceiptType, *,
};
use bytes::Bytes;
use ethereum_forkid::ForkId;
use fastrlp::*;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum EthMessageId {
    Status = 0,
    NewBlockHashes = 1,
    Transactions = 2,
    GetBlockHeaders = 3,
    BlockHeaders = 4,
    GetBlockBodies = 5,
    BlockBodies = 6,
    NewBlock = 7,
    NewPooledTransactionHashes = 8,
    GetPooledTransactions = 9,
    PooledTransactions = 10,
    GetNodeData = 13,
    NodeData = 14,
    GetReceipts = 15,
    Receipts = 16,
}

#[derive(RlpEncodable, RlpDecodable, Clone, Copy, PartialEq, Debug)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: BlockNumber,
}

#[derive(RlpEncodableWrapper, RlpDecodableWrapper, Clone, PartialEq, Debug)]
pub struct NewBlockHashesMessage(pub Vec<BlockHashAndNumber>);

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct BlockBodyType {
    pub transactions: Vec<MessageWithSignature>,
    pub ommers: Vec<HeaderType>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, Copy, PartialEq, Debug)]
pub struct GetBlockHeadersMessage {
    pub request_id: u64,
    pub params: GetBlockHeadersMessageParams,
}

#[derive(RlpEncodableWrapper, RlpDecodableWrapper, Clone, PartialEq, Debug)]
pub struct BlockReceipts(pub Vec<ReceiptType>);

#[derive(Clone, Copy, PartialEq, Debug, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeadersMessageParams {
    pub start_block: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: u8,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct BlockHeadersMessage {
    pub request_id: u64,
    pub headers: Vec<HeaderType>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct NewBlockMessage {
    pub block: Box<BlockType>,
    pub total_difficulty: u64,
}

#[derive(RlpEncodableWrapper, RlpDecodableWrapper, Clone, PartialEq, Debug)]
pub struct NewPooledTransactionHashesMessage(pub Vec<H256>);

#[derive(RlpEncodableWrapper, RlpDecodableWrapper, Clone, PartialEq, Debug)]
pub struct TransactionsMessage(pub Vec<MessageWithSignature>);

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct GetBlockBodiesMessage {
    pub request_id: u64,
    pub block_hashes: Vec<H256>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct BlockBodiesMessage {
    pub request_id: u64,
    pub block_bodies: Vec<BlockBodyType>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct GetPooledTransactionsMessage {
    pub request_id: u64,
    pub tx_hashes: Vec<H256>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct PooledTransactionsMessage {
    pub request_id: u64,
    pub transactions: Vec<MessageWithSignature>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct GetNodeDataMessage {
    pub request_id: u64,
    pub hashes: Vec<H256>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct NodeDataMessage {
    pub request_id: u64,
    pub data: Vec<Bytes>,
}
#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct GetReceiptsMessage {
    pub request_id: u64,
    pub block_hashes: Vec<H256>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct ReceiptsMessage {
    pub request_id: u64,
    pub receipts: Vec<BlockReceipts>,
}

#[derive(RlpEncodable, RlpDecodable, Clone, PartialEq, Debug)]
pub struct StatusMessage {
    pub protocol_version: usize,
    pub network_id: u64,
    pub total_difficulty: U256,
    pub best_hash: H256,
    pub genesis_hash: H256,
    pub fork_id: ForkId,
}

/// An Eth/66 protocol message
///
/// A full description of the [Eth/66 protocol](https://github.com/ethereum/devp2p/blob/master/caps/eth.md)
/// can be found on the Ethereum foundation's Github.
/// Eth/66 is an update to Eth/65. The changes from 65 to 66 are detailed in [EIP-2481](https://eips.ethereum.org/EIPS/eip-2481)
#[derive(Clone, PartialEq, Debug)]
pub enum Message {
    Status(StatusMessage),
    NewBlockHashes(NewBlockHashesMessage),
    Transactions(TransactionsMessage),
    GetBlockHeaders(GetBlockHeadersMessage),
    BlockHeaders(BlockHeadersMessage),
    GetBlockBodies(GetBlockBodiesMessage),
    BlockBodies(BlockBodiesMessage),
    NewBlock(NewBlockMessage),
    NewPooledTransactionHashes(NewPooledTransactionHashesMessage),
    GetPooledTransactions(GetPooledTransactionsMessage),
    PooledTransactions(PooledTransactionsMessage),
    GetNodeData(GetNodeDataMessage),
    NodeData(NodeDataMessage),
    GetReceipts(GetReceiptsMessage),
    Receipts(ReceiptsMessage),
}

impl Message {
    pub fn eth_id(&self) -> EthMessageId {
        match self {
            Message::Status(_) => EthMessageId::Status,
            Message::NewBlockHashes(_) => EthMessageId::NewBlockHashes,
            Message::Transactions(_) => EthMessageId::Transactions,
            Message::GetBlockHeaders(_) => EthMessageId::GetBlockHeaders,
            Message::BlockHeaders(_) => EthMessageId::BlockHeaders,
            Message::GetBlockBodies(_) => EthMessageId::GetBlockBodies,
            Message::BlockBodies(_) => EthMessageId::BlockBodies,
            Message::NewBlock(_) => EthMessageId::NewBlock,
            Message::NewPooledTransactionHashes(_) => EthMessageId::NewPooledTransactionHashes,
            Message::GetPooledTransactions(_) => EthMessageId::GetPooledTransactions,
            Message::PooledTransactions(_) => EthMessageId::PooledTransactions,
            Message::GetNodeData(_) => EthMessageId::GetNodeData,
            Message::NodeData(_) => EthMessageId::NodeData,
            Message::GetReceipts(_) => EthMessageId::GetReceipts,
            Message::Receipts(_) => EthMessageId::Receipts,
        }
    }
}
