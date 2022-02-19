use super::{
    chain_config::ChainConfig,
    messages::{EthMessageId, Message},
};
use crate::models::*;
use async_trait::async_trait;
use futures_core::Stream;
use std::{fmt::Debug, pin::Pin};

#[derive(Clone, Debug)]
pub struct Status {
    pub total_difficulty: U256,
    pub best_hash: H256,
    pub chain_fork_config: ChainConfig,
    pub max_block: BlockNumber,
}

pub type PeerId = H512;

#[derive(Clone, Debug)]
pub enum PeerFilter {
    MinBlock(u64),
    PeerId(PeerId),
    Random(u64 /* max peers */),
    All,
}

#[derive(Clone, Debug)]
pub struct MessageFromPeer {
    pub message: Message,
    pub from_peer_id: Option<PeerId>,
}

pub type MessageFromPeerStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<MessageFromPeer>> + Send>>;

#[async_trait]
pub trait SentryClient: Send + Debug {
    async fn set_status(&mut self, status: Status) -> anyhow::Result<()>;

    async fn penalize_peer(&mut self, peer_id: PeerId) -> anyhow::Result<()>;

    async fn send_message(
        &mut self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<u32>;

    async fn receive_messages(
        &mut self,
        filter_ids: &[EthMessageId],
    ) -> anyhow::Result<MessageFromPeerStream>;
}
