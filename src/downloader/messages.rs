use crate::downloader::block_id::BlockId;
use ethereum_types::H256;

#[derive(Debug)]
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

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, Copy)]
pub struct GetBlockHeadersMessage {
    pub request_id: u64,
    pub start_block: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: bool,
}

#[derive(rlp_derive::RlpEncodable, rlp_derive::RlpDecodable, Clone, Copy)]
pub struct BlockHashAndNumber(pub H256, pub u64);

#[derive(rlp_derive::RlpEncodableWrapper, rlp_derive::RlpDecodableWrapper, Clone)]
pub struct NewBlockHashesMessage {
    pub ids: Vec<BlockHashAndNumber>,
}

#[derive(Clone)]
pub enum Message {
    GetBlockHeaders(GetBlockHeadersMessage),
    NewBlockHashes(NewBlockHashesMessage),
}

impl Message {
    pub fn eth_id(self: &Message) -> EthMessageId {
        match self {
            Message::GetBlockHeaders(_) => EthMessageId::GetBlockHeaders,
            Message::NewBlockHashes(_) => EthMessageId::NewBlockHashes,
        }
    }
}
