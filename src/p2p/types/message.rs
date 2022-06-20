use crate::{
    models::{BlockBody, MessageWithSignature, H256},
    p2p::types::*,
    sentry::devp2p::PeerId,
};
use anyhow::anyhow;
use ethereum_interfaces::sentry as grpc_sentry;
use fastrlp::*;
use rand::Rng;
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum MessageId {
    Status = 0,
    NewBlockHashes = 1,
    NewBlock = 2,
    Transactions = 3,
    NewPooledTransactionHashes = 4,
    GetBlockHeaders = 5,
    GetBlockBodies = 6,
    GetNodeData = 7,
    GetReceipts = 8,
    GetPooledTransactions = 9,
    BlockHeaders = 10,
    BlockBodies = 11,
    NodeData = 12,
    Receipts = 13,
    PooledTransactions = 14,
}

#[derive(Debug)]
pub struct InvalidMessageId(grpc_sentry::MessageId);

impl std::error::Error for InvalidMessageId {}

impl Display for InvalidMessageId {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid message id: {:?}", self.0)
    }
}

impl TryFrom<grpc_sentry::MessageId> for MessageId {
    type Error = InvalidMessageId;

    #[inline(always)]
    fn try_from(msg_id: grpc_sentry::MessageId) -> Result<Self, Self::Error> {
        match msg_id {
            grpc_sentry::MessageId::Status66 => Ok(MessageId::Status),
            grpc_sentry::MessageId::NewBlockHashes66 => Ok(MessageId::NewBlockHashes),
            grpc_sentry::MessageId::Transactions66 => Ok(MessageId::Transactions),
            grpc_sentry::MessageId::GetBlockHeaders66 => Ok(MessageId::GetBlockHeaders),
            grpc_sentry::MessageId::BlockHeaders66 => Ok(MessageId::BlockHeaders),
            grpc_sentry::MessageId::GetBlockBodies66 => Ok(MessageId::GetBlockBodies),
            grpc_sentry::MessageId::BlockBodies66 => Ok(MessageId::BlockBodies),
            grpc_sentry::MessageId::NewBlock66 => Ok(MessageId::NewBlock),
            grpc_sentry::MessageId::NewPooledTransactionHashes66 => {
                Ok(MessageId::NewPooledTransactionHashes)
            }
            grpc_sentry::MessageId::GetPooledTransactions66 => Ok(MessageId::GetPooledTransactions),
            grpc_sentry::MessageId::PooledTransactions66 => Ok(MessageId::PooledTransactions),
            grpc_sentry::MessageId::GetNodeData66 => Ok(MessageId::GetNodeData),
            grpc_sentry::MessageId::NodeData66 => Ok(MessageId::NodeData),
            grpc_sentry::MessageId::GetReceipts66 => Ok(MessageId::GetReceipts),
            grpc_sentry::MessageId::Receipts66 => Ok(MessageId::Receipts),
            _ => Err(InvalidMessageId(msg_id)),
        }
    }
}

impl From<MessageId> for grpc_sentry::MessageId {
    #[inline(always)]
    fn from(id: MessageId) -> Self {
        match id {
            MessageId::Status => grpc_sentry::MessageId::Status66,
            MessageId::NewBlockHashes => grpc_sentry::MessageId::NewBlockHashes66,
            MessageId::Transactions => grpc_sentry::MessageId::Transactions66,
            MessageId::GetBlockHeaders => grpc_sentry::MessageId::GetBlockHeaders66,
            MessageId::BlockHeaders => grpc_sentry::MessageId::BlockHeaders66,
            MessageId::GetBlockBodies => grpc_sentry::MessageId::GetBlockBodies66,
            MessageId::BlockBodies => grpc_sentry::MessageId::BlockBodies66,
            MessageId::NewBlock => grpc_sentry::MessageId::NewBlock66,
            MessageId::NewPooledTransactionHashes => {
                grpc_sentry::MessageId::NewPooledTransactionHashes66
            }
            MessageId::GetPooledTransactions => grpc_sentry::MessageId::GetPooledTransactions66,
            MessageId::PooledTransactions => grpc_sentry::MessageId::PooledTransactions66,
            MessageId::GetNodeData => grpc_sentry::MessageId::GetNodeData66,
            MessageId::NodeData => grpc_sentry::MessageId::NodeData66,
            MessageId::GetReceipts => grpc_sentry::MessageId::GetReceipts66,
            MessageId::Receipts => grpc_sentry::MessageId::Receipts66,
        }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewPooledTransactionHashes(pub Vec<H256>);

#[derive(Debug, Clone, Eq, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct Transactions(pub Vec<MessageWithSignature>);

#[derive(Debug, Clone, Eq, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetPooledTransactions {
    pub request_id: u64,
    pub hashes: Vec<H256>,
}

#[derive(Debug, Clone, Eq, PartialEq, RlpEncodable, RlpDecodable)]
pub struct PooledTransactions {
    pub request_id: u64,
    pub transactions: Vec<MessageWithSignature>,
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockBodies {
    pub request_id: u64,
    pub bodies: Vec<BlockBody>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    NewBlockHashes(NewBlockHashes),
    GetBlockHeaders(GetBlockHeaders),
    GetBlockBodies(GetBlockBodies),
    BlockBodies(BlockBodies),
    BlockHeaders(BlockHeaders),
    NewBlock(Box<NewBlock>),
    NewPooledTransactionHashes(NewPooledTransactionHashes),
    Transactions(Transactions),
    GetPooledTransactions(GetPooledTransactions),
    PooledTransactions(PooledTransactions),
}

impl Message {
    #[inline(always)]
    pub const fn id(&self) -> MessageId {
        match self {
            Self::NewBlockHashes(_) => MessageId::NewBlockHashes,
            Self::GetBlockHeaders(_) => MessageId::GetBlockHeaders,
            Self::GetBlockBodies(_) => MessageId::GetBlockBodies,
            Self::BlockBodies(_) => MessageId::BlockBodies,
            Self::BlockHeaders(_) => MessageId::BlockHeaders,
            Self::NewBlock(_) => MessageId::NewBlock,
            Self::NewPooledTransactionHashes(_) => MessageId::NewPooledTransactionHashes,
            Self::Transactions(_) => MessageId::Transactions,
            Self::GetPooledTransactions(_) => MessageId::GetPooledTransactions,
            Self::PooledTransactions(_) => MessageId::PooledTransactions,
        }
    }
}

impl From<HeaderRequest> for Message {
    #[inline(always)]
    fn from(req: HeaderRequest) -> Self {
        Message::GetBlockHeaders(GetBlockHeaders {
            request_id: rand::thread_rng().gen::<u64>(),
            params: GetBlockHeadersParams {
                start: req.start,
                limit: req.limit,
                skip: req.skip,
                reverse: if req.reverse { 1 } else { 0 },
            },
        })
    }
}

impl From<Vec<H256>> for Message {
    #[inline(always)]
    fn from(hashes: Vec<H256>) -> Self {
        Message::GetBlockBodies(GetBlockBodies {
            request_id: rand::thread_rng().gen::<u64>(),
            hashes,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InboundMessage {
    pub msg: Message,
    pub peer_id: PeerId,
    pub sentry_id: usize,
}

impl InboundMessage {
    #[inline]
    pub fn new(value: grpc_sentry::InboundMessage, sentry_id: usize) -> anyhow::Result<Self> {
        let msg_data_slice = &mut &*value.data;
        let msg = match MessageId::try_from(match grpc_sentry::MessageId::from_i32(value.id) {
            Some(msg_id) => msg_id,
            _ => return Err(anyhow!("Unsupported message id: {}", value.id)),
        })? {
            MessageId::NewBlockHashes => {
                Message::NewBlockHashes(Decodable::decode(msg_data_slice)?)
            }
            MessageId::NewBlock => Message::NewBlock(Box::new(Decodable::decode(msg_data_slice)?)),
            MessageId::Transactions => Message::Transactions(Decodable::decode(msg_data_slice)?),
            MessageId::NewPooledTransactionHashes => {
                Message::NewPooledTransactionHashes(Decodable::decode(msg_data_slice)?)
            }
            MessageId::GetBlockHeaders => {
                Message::GetBlockHeaders(Decodable::decode(msg_data_slice)?)
            }
            MessageId::GetBlockBodies => {
                Message::GetBlockBodies(Decodable::decode(msg_data_slice)?)
            }
            MessageId::GetNodeData => todo!(),
            MessageId::GetReceipts => todo!(),
            MessageId::GetPooledTransactions => {
                Message::GetPooledTransactions(Decodable::decode(msg_data_slice)?)
            }
            MessageId::BlockHeaders => Message::BlockHeaders(Decodable::decode(msg_data_slice)?),
            MessageId::BlockBodies => Message::BlockBodies(Decodable::decode(msg_data_slice)?),
            MessageId::NodeData => todo!(),
            MessageId::Receipts => todo!(),
            MessageId::PooledTransactions => {
                Message::PooledTransactions(Decodable::decode(msg_data_slice)?)
            }
            _ => todo!(),
        };
        Ok(InboundMessage {
            msg,
            peer_id: value.peer_id.unwrap_or_default().into(),
            sentry_id,
        })
    }
}
