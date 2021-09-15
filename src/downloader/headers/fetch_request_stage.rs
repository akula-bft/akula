use crate::{
    downloader::{
        block_id,
        headers::{
            header_slices,
            header_slices::{HeaderSliceStatus, HeaderSlices},
        },
        messages::{GetBlockHeadersMessage, GetBlockHeadersMessageParams, Message},
        sentry_client::PeerFilter,
        sentry_client_reactor::{SendMessageError, SentryClientReactor},
    },
    models::BlockNumber,
};
use parking_lot::{lock_api::RwLockUpgradableReadGuard, RwLock};
use std::{
    cell::{Cell, RefCell},
    ops::DerefMut,
    sync::Arc,
    time,
};
use tokio::sync::watch;
use tracing::*;

/// Sends requests to P2P via sentry to get the slices. Slices become Waiting.
pub struct FetchRequestStage {
    header_slices: Arc<HeaderSlices>,
    sentry: Arc<RwLock<SentryClientReactor>>,
    last_request_id: Cell<u64>,
    pending_watch: RefCell<watch::Receiver<usize>>,
}

impl FetchRequestStage {
    pub fn new(header_slices: Arc<HeaderSlices>, sentry: Arc<RwLock<SentryClientReactor>>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Empty);

        Self {
            header_slices,
            sentry,
            last_request_id: Cell::new(0),
            pending_watch: RefCell::new(pending_watch),
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Empty)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("FetchRequestStage: start");
        if self.pending_count() == 0 {
            debug!("FetchRequestStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
            }
            debug!("FetchRequestStage: waiting pending done");
        }

        info!(
            "FetchRequestStage: requesting {} slices",
            self.pending_count()
        );
        self.request_pending()?;
        debug!("FetchRequestStage: done");
        Ok(())
    }

    fn request_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Empty {
                let mut request_id = self.last_request_id.get();
                request_id += 1;
                self.last_request_id.set(request_id);

                let block_num = slice.start_block_num;
                let limit = header_slices::HEADER_SLICE_SIZE as u64 + 1;

                let result = self.request(request_id, block_num, limit);
                match result {
                    Err(error) => match error.downcast_ref::<SendMessageError>() {
                        Some(SendMessageError::SendQueueFull) => return Some(Ok(())),
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
