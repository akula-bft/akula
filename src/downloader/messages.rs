use crate::downloader::block_id::BlockId;

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

#[derive(Clone, Copy)]
pub enum Message {
    GetBlockHeaders(GetBlockHeadersMessage),
}

impl From<Message> for EthMessageId {
    fn from(message: Message) -> Self {
        match message {
            Message::GetBlockHeaders(_) => EthMessageId::GetBlockHeaders,
        }
    }
}
