use crate::{
    models::BlockNumber,
    sentry_connector::{
        messages::{EthMessageId, Message, NewBlockHashesMessage},
        sentry_client::PeerId,
        sentry_client_reactor::*,
    },
};
use futures_core::Stream;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    ops::Deref,
    pin::Pin,
    sync::{atomic::*, Arc},
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::StreamExt;
use tracing::*;

struct NewBlockHashesMessageFromPeer {
    message: NewBlockHashesMessage,
    from_peer_id: Option<PeerId>,
}

type NewBlockHashesMessageStream =
    Pin<Box<dyn Stream<Item = NewBlockHashesMessageFromPeer> + Send>>;

/// Listen to new block hashes announces to estimate the current top block number.
pub struct TopBlockEstimateStage {
    sentry: SentryClientReactorShared,
    is_over: Arc<AtomicBool>,
    message_stream: AsyncMutex<Option<NewBlockHashesMessageStream>>,
    peer_top_blocks: Arc<Mutex<HashMap<PeerId, BlockNumber>>>,
}

impl TopBlockEstimateStage {
    pub fn new(sentry: SentryClientReactorShared) -> Self {
        Self {
            sentry,
            is_over: Arc::new(false.into()),
            message_stream: AsyncMutex::new(None),
            peer_top_blocks: Arc::new(Mutex::new(HashMap::<PeerId, BlockNumber>::new())),
        }
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        // when it is over - hang to avoid a live idle loop
        // ideally stop calling this execute() in the make_stage_stream loop
        if self.is_over.load(Ordering::SeqCst) {
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        }

        let mut message_stream = self.message_stream.try_lock()?;
        if message_stream.is_none() {
            let sentry = self.sentry.read().await;
            *message_stream = Some(self.receive_messages(&sentry)?);
        }

        let message_result = message_stream.as_mut().unwrap().next().await;
        match message_result {
            Some(message) => self.on_message(message),
            None => self.is_over.store(true, Ordering::SeqCst),
        }
        Ok(())
    }

    fn on_message(&self, message_from_peer: NewBlockHashesMessageFromPeer) {
        debug!("TopBlockEstimateStage: received new block hashes");
        if message_from_peer.message.0.is_empty() {
            return;
        }
        if message_from_peer.from_peer_id.is_none() {
            return;
        }
        let from_peer_id = message_from_peer.from_peer_id.unwrap();

        let mut block_nums = message_from_peer
            .message
            .0
            .iter()
            .map(|id| id.number)
            .collect::<Vec<BlockNumber>>();
        let mut peer_top_blocks = self.peer_top_blocks.lock();
        if let Some(current_peer_top_block) = peer_top_blocks.get(&from_peer_id) {
            block_nums.push(*current_peer_top_block);
        }

        let peer_top_block = block_nums.iter().max_by_key(|num| num.0).unwrap();
        peer_top_blocks.insert(from_peer_id, *peer_top_block);
    }

    fn receive_messages(
        &self,
        sentry: &SentryClientReactor,
    ) -> anyhow::Result<NewBlockHashesMessageStream> {
        let in_stream = sentry.receive_messages(EthMessageId::NewBlockHashes)?;

        let out_stream = in_stream.map(|message_from_peer| match message_from_peer.message {
            Message::NewBlockHashes(message) => NewBlockHashesMessageFromPeer {
                message,
                from_peer_id: message_from_peer.from_peer_id,
            },
            _ => panic!("unexpected type {:?}", message_from_peer.message.eth_id()),
        });

        Ok(Box::pin(out_stream))
    }

    pub fn is_over(&self) -> bool {
        self.is_over.load(Ordering::SeqCst)
    }

    pub fn estimated_top_block_num_provider(&self) -> impl Fn() -> Option<BlockNumber> {
        let peer_top_blocks = self.peer_top_blocks.clone();
        move || -> Option<BlockNumber> {
            Self::average_peer_block_num(peer_top_blocks.lock().deref())
        }
    }

    pub fn estimated_top_block_num(&self) -> Option<BlockNumber> {
        self.estimated_top_block_num_provider()()
    }

    fn average_peer_block_num(peer_blocks: &HashMap<PeerId, BlockNumber>) -> Option<BlockNumber> {
        if peer_blocks.is_empty() {
            return None;
        }

        let block_nums = peer_blocks.values().map(|num| num.0);
        let average_block_num = BlockNumber(block_nums.sum::<u64>() / (peer_blocks.len() as u64));
        Some(average_block_num)
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let is_over = self.is_over.clone();
        move || -> bool { !is_over.load(Ordering::SeqCst) }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for TopBlockEstimateStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
