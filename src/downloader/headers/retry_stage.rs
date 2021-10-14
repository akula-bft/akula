use crate::downloader::headers::header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc, time, time::Duration};
use tokio::sync::watch;
use tracing::*;

/// Handles timeouts. If a slice is Waiting for too long, we need to request it again.
/// Status is updated to Empty (the slice will be processed by the FetchRequestStage again).
pub struct RetryStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: watch::Receiver<usize>,
}

impl RetryStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            pending_watch: header_slices.watch_status_changes(HeaderSliceStatus::Waiting),
            header_slices,
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Waiting)
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("RetryStage: start");
        if self.pending_count() == 0 {
            debug!("RetryStage: waiting pending");
            while *self.pending_watch.borrow_and_update() == 0 {
                self.pending_watch.changed().await?;
            }
            debug!("RetryStage: waiting pending done");
        }

        // don't retry more often than once per 1 sec
        tokio::time::sleep(Duration::from_secs(1)).await;

        let count = self.reset_pending()?;
        if count > 0 {
            info!("RetryStage: did reset {} slices for retry", count);
        }
        debug!("RetryStage: done");
        Ok(())
    }

    fn reset_pending(&self) -> anyhow::Result<usize> {
        let now = time::Instant::now();
        let mut count: usize = 0;
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if (slice.status == HeaderSliceStatus::Waiting)
                && RetryStage::is_waiting_timeout_expired(&slice, &now)
            {
                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                slice.request_time = None;
                slice.request_attempt += 1;
                self.header_slices
                    .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Empty);
                count += 1;
            }
            None
        })?;
        Ok(count)
    }

    fn is_waiting_timeout_expired(slice: &HeaderSlice, now: &time::Instant) -> bool {
        if slice.request_time.is_none() {
            return false;
        }
        let request_time = slice.request_time.unwrap();
        let elapsed = now.duration_since(request_time);
        let timeout = RetryStage::timeout_for_attempt(slice.request_attempt);
        elapsed > timeout
    }

    fn timeout_for_attempt(attempt: u16) -> Duration {
        match attempt {
            0 => Duration::from_secs(5),
            1 => Duration::from_secs(10),
            2 => Duration::from_secs(15),
            _ => Duration::from_secs(30),
        }
    }
}
