use crate::downloader::headers::{
    header_slices,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    preverified_hashes_config::PreverifiedHashesConfig,
};
use parking_lot::lock_api::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc};
use tokio::sync::watch;
use tracing::*;

/// Checks that block hashes are matching the expected ones and sets Verified status.
pub struct VerifyStage {
    header_slices: Arc<HeaderSlices>,
    preverified_hashes: PreverifiedHashesConfig,
    pending_watch: watch::Receiver<usize>,
}

impl VerifyStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        preverified_hashes: PreverifiedHashesConfig,
    ) -> Self {
        Self {
            pending_watch: header_slices.watch_status_changes(HeaderSliceStatus::Downloaded),
            header_slices,
            preverified_hashes,
        }
    }

    fn pending_count(&self) -> usize {
        self.header_slices
            .count_slices_in_status(HeaderSliceStatus::Downloaded)
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("VerifyStage: start");
        if self.pending_count() == 0 {
            debug!("VerifyStage: waiting pending");
            while *self.pending_watch.borrow_and_update() == 0 {
                self.pending_watch.changed().await?;
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

    /// The algorithm verifies that the top of the slice matches one of the preverified hashes,
    /// and that all blocks down to the root of the slice are connected by the parent_hash field.
    ///
    /// For example, if we have a HeaderSlice[192...384]
    /// (with block headers from 192 to 384 inclusive), it verifies that:
    ///
    /// hash(slice[384]) == preverified hash(384)
    /// hash(slice[383]) == slice[384].parent_hash
    /// hash(slice[382]) == slice[383].parent_hash
    /// ...
    /// hash(slice[192]) == slice[193].parent_hash
    ///
    /// Thus verifying hashes of all the headers.
    fn verify_slice(&self, slice: &HeaderSlice) -> bool {
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();

        if headers.is_empty() {
            return true;
        }

        let last = headers.last().unwrap();
        let last_hash = last.hash();
        let expected_last_hash =
            self.preverified_hash(slice.start_block_num.0 + headers.len() as u64 - 1);
        if expected_last_hash.is_none() {
            return false;
        }
        if last_hash != *expected_last_hash.unwrap() {
            return false;
        }

        for child_index in (1..headers.len()).rev() {
            let parent_index = child_index - 1;

            let child = &headers[child_index];
            let parent = &headers[parent_index];

            let parent_hash = parent.hash();
            let expected_parent_hash = child.parent_hash;
            if parent_hash != expected_parent_hash {
                return false;
            }
        }

        true
    }

    fn preverified_hash(&self, block_num: u64) -> Option<&ethereum_types::H256> {
        let preverified_step_size = header_slices::HEADER_SLICE_SIZE as u64;
        if block_num % preverified_step_size != 0 {
            return None;
        }
        let index = block_num / preverified_step_size;
        self.preverified_hashes.hashes.get(index as usize)
    }
}
