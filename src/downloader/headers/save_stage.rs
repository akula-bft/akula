use crate::downloader::headers::header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{cell::RefCell, ops::DerefMut, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Saves slices into the database, and sets Saved status.
pub struct SaveStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: RefCell<watch::Receiver<usize>>,
}

impl SaveStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Verified);

        Self {
            header_slices,
            pending_watch: RefCell::new(pending_watch),
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Verified)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("SaveStage: start");
        if self.pending_count() == 0 {
            debug!("SaveStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
            }
            debug!("SaveStage: waiting pending done");
        }

        info!("SaveStage: saving {} slices", self.pending_count());
        self.save_pending().await?;
        debug!("SaveStage: done");
        Ok(())
    }

    async fn save_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Verified {
                let save_result = self.save_slice(&slice);
                if save_result.is_err() {
                    return Some(save_result);
                }

                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                self.header_slices
                    .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Saved);
            }
            None
        })
    }

    fn save_slice(&self, _slice: &HeaderSlice) -> anyhow::Result<()> {
        // TODO: save verified headers to the DB
        Ok(())
    }
}
