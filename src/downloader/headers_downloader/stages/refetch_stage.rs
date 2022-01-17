use super::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSliceStatus, HeaderSlices},
};
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc};
use tracing::*;

/// Handles retries to fetch some slices.
/// Status is updated to Empty (the slice will be processed by the FetchRequestStage again).
pub struct RefetchStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
}

impl RefetchStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Refetch,
                header_slices,
                "RefetchStage",
            ),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.pending_watch.wait().await?;

        let count = self.reset_pending()?;
        if count > 0 {
            debug!("RefetchStage: did reset {} slices for retry", count);
        }

        Ok(())
    }

    fn reset_pending(&self) -> anyhow::Result<usize> {
        let mut count: usize = 0;
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Refetch {
                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                slice.refetch_attempt += 1;
                self.header_slices
                    .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Empty);
                count += 1;
            }
        });
        Ok(count)
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for RefetchStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
}
