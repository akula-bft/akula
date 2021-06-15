use crate::downloader::chain_config::ChainConfig;
use async_trait::async_trait;
use futures_core::Stream;

pub struct Status {
    pub total_difficulty: ethereum_types::U256,
    pub best_hash: ethereum_types::H256,
    pub chain_fork_config: ChainConfig,
    pub max_block: u64,
}

pub enum PeerFilter {
    MinBlock(u64),
    PeerId(ethereum_types::H512),
    Random(u64 /* max peers */),
    All,
}

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

pub trait Identifiable {
    fn id(&self) -> EthMessageId;
}

pub trait Message: Identifiable + Send + rlp::Encodable {}

pub struct MessageFromPeer {
    pub message: Box<dyn Message>,
    pub from_peer_id: Option<ethereum_types::H512>,
}

#[async_trait]
pub trait SentryClient {
    async fn set_status(&mut self, status: Status) -> anyhow::Result<()>;

    //async fn penalize_peer(&mut self) -> anyhow::Result<()>;
    //async fn peer_min_block(&mut self) -> anyhow::Result<()>;

    async fn send_message(
        &mut self,
        message: Box<dyn Message>,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()>;

    async fn receive_messages(
        &mut self,
    ) -> anyhow::Result<Box<dyn Stream<Item = anyhow::Result<MessageFromPeer>> + Unpin>>;
}
