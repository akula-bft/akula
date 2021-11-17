use crate::{
    downloader::headers::{
        header_slice_status_watch::HeaderSliceStatusWatch,
        header_slices::{HeaderSliceStatus, HeaderSlices},
    },
    models::BlockNumber,
    sentry::{
        block_id,
        messages::{GetBlockHeadersMessage, GetBlockHeadersMessageParams, Message},
        sentry_client::PeerFilter,
        sentry_client_reactor::{SendMessageError, SentryClientReactor},
    },
};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
    ops::DerefMut,
    sync::{atomic::*, Arc},
    time,
};
use tracing::*;

/// Sends requests to P2P via sentry to get the slices. Slices become Waiting.
pub struct FetchRequestStage {
    header_slices: Arc<HeaderSlices>,
    sentry: Arc<RwLock<SentryClientReactor>>,
    slice_size: usize,
    pending_watch: HeaderSliceStatusWatch,
    last_request_id: AtomicU64,
}

impl FetchRequestStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        sentry: Arc<RwLock<SentryClientReactor>>,
        slice_size: usize,
    ) -> Self {
        Self {
            header_slices: header_slices.clone(),
            sentry,
            slice_size,
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Empty,
                header_slices,
                "FetchRequestStage",
            ),
            last_request_id: 0.into(),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("FetchRequestStage: start");
        self.pending_watch.wait().await?;

        debug!(
            "FetchRequestStage: requesting {} slices",
            self.pending_watch.pending_count()
        );
        self.request_pending()?;

        // in case of SendQueueFull, await for extra capacity
        if self.pending_watch.pending_count() > 0 {
            // obtain the sentry lock, and release it before awaiting
            let capacity_future = {
                let sentry = self.sentry.read();
                sentry.reserve_capacity_in_send_queue()
            };
            capacity_future.await?;
        }

        debug!("FetchRequestStage: done");
        Ok(())
    }

    fn request_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Empty {
                let request_id = self.last_request_id.fetch_add(1, Ordering::SeqCst);

                let block_num = slice.start_block_num;
                let limit = self.slice_size as u64;

                let result = self.request(request_id, block_num, limit);
                match result {
                    Err(error) => match error.downcast_ref::<SendMessageError>() {
                        Some(SendMessageError::SendQueueFull) => {
                            debug!("FetchRequestStage: request send queue is full");
                            return Some(Ok(()));
                        }
                        Some(SendMessageError::ReactorStopped) => return Some(Err(error)),
                        None => return Some(Err(error)),
                    },
                    Ok(_) => {
                        let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                        slice.request_time = Some(time::Instant::now());
                        self.header_slices
                            .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Waiting);
                    }
                }
            }
            None
        })
    }

    fn request(&self, request_id: u64, block_num: BlockNumber, limit: u64) -> anyhow::Result<()> {
        let message = GetBlockHeadersMessage {
            request_id,
            params: GetBlockHeadersMessageParams {
                start_block: block_id::BlockId::Number(block_num),
                limit,
                skip: 0,
                reverse: 0,
            },
        };
        self.sentry
            .read()
            .try_send_message(Message::GetBlockHeaders(message), PeerFilter::Random(1))
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for FetchRequestStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        FetchRequestStage::execute(self).await
    }
}
