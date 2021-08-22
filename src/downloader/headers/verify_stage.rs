use crate::downloader::headers::header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{cell::RefCell, ops::DerefMut, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Checks that block hashes are matching the expected ones and sets Verified status.
pub struct VerifyStage {
    header_slices: Arc<HeaderSlices>,
    pending_watch: RefCell<watch::Receiver<usize>>,
}

impl VerifyStage {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        let pending_watch = header_slices.watch_status_changes(HeaderSliceStatus::Downloaded);

        Self {
            header_slices,
            pending_watch: RefCell::new(pending_watch),
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Downloaded)
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        debug!("VerifyStage: start");
        if self.pending_count() == 0 {
            debug!("VerifyStage: waiting pending");
            let mut watch = self.pending_watch.borrow_mut();
            while *watch.borrow_and_update() == 0 {
                watch.changed().await?;
            }
            debug!("VerifyStage: waiting pending done");
        }

        info!("VerifyStage: verifying {} slices", self.pending_count());
        self.verify_pending()?;
        debug!("VerifyStage: done");
        Ok(())
    }

    fn verify_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Downloaded {
                let is_verified = self.verify_slice(&slice);

                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                if is_verified {
                    self.header_slices
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Verified);
                } else {
                    self.header_slices
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Empty);
                    slice.headers = None;
                    // TODO: penalize peer?
                }
            }
            None
        })
    }

    fn verify_slice(&self, _slice: &HeaderSlice) -> bool {
        // TODO: verify hashes properly
        rand::random::<u8>() < 224
    }
}
