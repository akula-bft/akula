use crate::downloader::block_id::BlockId;
use ethereum::{Block as BlockType, Header as HeaderType};
use ethereum_types::H256;

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

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, Copy, PartialEq, Debug)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: u64,
}

#[derive(
    rlp_derive::RlpEncodableWrapper, rlp_derive::RlpDecodableWrapper, Clone, PartialEq, Debug,
)]
pub struct NewBlockHashesMessage {
    pub ids: Vec<BlockHashAndNumber>,
}

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, Copy, PartialEq, Debug)]
pub struct GetBlockHeadersMessage {
    pub request_id: u64,
    pub params: GetBlockHeadersMessageParams,
}

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, Copy, PartialEq, Debug)]
pub struct GetBlockHeadersMessageParams {
    pub start_block: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: u8,
}

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, PartialEq, Debug)]
pub struct BlockHeadersMessage {
    pub request_id: u64,
    pub headers: Vec<HeaderType>,
}

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, PartialEq, Debug)]
pub struct NewBlockMessage {
    pub block: Box<BlockType>,
    pub total_difficulty: u64,
}

#[derive(
    rlp_derive::RlpEncodableWrapper, rlp_derive::RlpDecodableWrapper, Clone, PartialEq, Debug,
)]
pub struct NewPooledTransactionHashesMessage {
    pub ids: Vec<H256>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum Message {
    NewBlockHashes(NewBlockHashesMessage),
    GetBlockHeaders(GetBlockHeadersMessage),
    BlockHeaders(BlockHeadersMessage),
    NewBlock(NewBlockMessage),
    NewPooledTransactionHashes(NewPooledTransactionHashesMessage),
}

impl Message {
    pub fn eth_id(self: &Message) -> EthMessageId {
        match self {
            Message::NewBlockHashes(_) => EthMessageId::NewBlockHashes,
            Message::GetBlockHeaders(_) => EthMessageId::GetBlockHeaders,
            Message::BlockHeaders(_) => EthMessageId::BlockHeaders,
            Message::NewBlock(_) => EthMessageId::NewBlock,
            Message::NewPooledTransactionHashes(_) => EthMessageId::NewPooledTransactionHashes,
        }
    }
}
