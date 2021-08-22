use crate::downloader::headers::header_slices::{HeaderSliceStatus, HeaderSlices};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{cell::RefCell, ops::DerefMut, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Handles timeouts. If a slice is Waiting for too long, we need to request it again.
/// Status is updated to Empty (the slice will be processed by the FetchRequestStage again).
pub struct RetryStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: RefCell<watch::Receiver<usize>>,
}

impl RetryStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Waiting);

        Self {
            header_slices,
            pending_watch: RefCell::new(pending_watch),
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Waiting)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("RetryStage: start");
        if self.pending_count() == 0 {
            debug!("RetryStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
            }
            debug!("RetryStage: waiting pending done");
        }

        // retry if nothing happens for 10 sec
        // TODO: ideally need to respect the time when Waiting was started,
        //     and only reset those that wait for too long
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        info!("RetryStage: resetting {} slices", self.pending_count());
        self.reset_pending()?;
        debug!("RetryStage: done");
        Ok(())
    }

    fn reset_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Waiting {
                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                self.header_slices
                    .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Empty);
            }
            None
        })
    }
}
