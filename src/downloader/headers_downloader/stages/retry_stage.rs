use super::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc, time, time::Duration};
use tracing::*;

/// Handles timeouts. If a slice is Waiting for too long, we need to request it again.
/// Status is updated to Empty (the slice will be processed by the FetchRequestStage again).
pub struct RetryStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
}

impl RetryStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Waiting,
                header_slices,
                "RetryStage",
            ),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("RetryStage: start");
        self.pending_watch.wait().await?;

        // don't retry more often than once per 1 sec
        tokio::time::sleep(Duration::from_secs(1)).await;

        let count = self.reset_pending()?;
        if count > 0 {
            debug!("RetryStage: did reset {} slices for retry", count);
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
        });
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

#[async_trait::async_trait]
impl super::stage::Stage for RetryStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        RetryStage::execute(self).await
    }
}
