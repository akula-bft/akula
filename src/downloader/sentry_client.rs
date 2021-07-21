use crate::downloader::{
    chain_config::ChainConfig,
    messages::{EthMessageId, Message},
};
use async_trait::async_trait;
use futures_core::Stream;

pub struct Status {
    pub total_difficulty: ethereum_types::U256,
    pub best_hash: ethereum_types::H256,
    pub chain_fork_config: ChainConfig,
    pub max_block: u64,
}

#[derive(Debug)]
pub enum PeerFilter {
    MinBlock(u64),
    PeerId(ethereum_types::H512),
    Random(u64 /* max peers */),
    All,
}

#[derive(Clone)]
pub struct MessageFromPeer {
    pub message: Message,
    pub from_peer_id: Option<ethereum_types::H512>,
}

#[async_trait]
pub trait SentryClient: Send {
    async fn set_status(&mut self, status: Status) -> anyhow::Result<()>;

    //async fn penalize_peer(&mut self) -> anyhow::Result<()>;
    //async fn peer_min_block(&mut self) -> anyhow::Result<()>;

    async fn send_message(
        &mut self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<u32>;

    async fn receive_messages(
        &mut self,
        filter_ids: &[EthMessageId],
    ) -> anyhow::Result<Box<dyn Stream<Item = anyhow::Result<MessageFromPeer>> + Unpin + Send>>;
}
