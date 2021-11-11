use super::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc, time::SystemTime};
use tracing::*;

/// Checks that block hashes are matching the expected ones and sets Verified status.
pub struct VerifyStageLinear {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
}

impl VerifyStageLinear {
    pub fn new(header_slices: Arc<HeaderSlices>) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Downloaded,
                header_slices,
                "VerifyStageLinear",
            ),
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("VerifyStageLinear: start");
        self.pending_watch.wait().await?;

        debug!(
            "VerifyStageLinear: verifying {} slices",
            self.pending_watch.pending_count()
        );
        self.verify_pending()?;
        debug!("VerifyStageLinear: done");
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

    fn now_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn verify_slice(&self, slice: &HeaderSlice) -> bool {
        self.verify_slice_is_linked_by_parent_hash(slice)
            && self.verify_slice_block_nums(slice)
            && self.verify_slice_timestamps(slice, Self::now_timestamp())
            && self.verify_slice_difficulties(slice)
            && self.verify_slice_pow(slice)
    }

    /// Verify that all blocks in the slice are linked by the parent_hash field.
    fn verify_slice_is_linked_by_parent_hash(&self, slice: &HeaderSlice) -> bool {
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();

        if headers.is_empty() {
            return true;
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

    /// Verify that block numbers start from the expected
    /// slice.start_block_num and increase sequentially.
    fn verify_slice_block_nums(&self, slice: &HeaderSlice) -> bool {
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();

        let mut expected_block_num = slice.start_block_num.0;
        for header in headers {
            if header.number.0 != expected_block_num {
                return false;
            }
            expected_block_num += 1;
        }

        true
    }

    /// Verify that timestamps are in the past and increase monotonically.
    fn verify_slice_timestamps(&self, slice: &HeaderSlice, max_timestamp: u64) -> bool {
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();

        if headers.is_empty() {
            return true;
        }

        for parent_index in 0..(headers.len() - 1) {
            let child_index = parent_index + 1;

            let parent = &headers[parent_index];
            let child = &headers[child_index];

            let parent_timestamp = parent.timestamp;
            let child_timestamp = child.timestamp;

            if parent_timestamp >= child_timestamp {
                return false;
            }
        }

        let last = headers.last().unwrap();
        let last_timestamp = last.timestamp;
        last_timestamp < max_timestamp
    }

    /// Verify that difficulty field is calculated properly.
    fn verify_slice_difficulties(&self, _slice: &HeaderSlice) -> bool {
        // TODO: verify_slice_difficulties
        true
    }

    /// Verify the headers proof-of-work.
    fn verify_slice_pow(&self, _slice: &HeaderSlice) -> bool {
        // TODO: verify_slice_pow
        true
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyStageLinear {
    async fn execute(&mut self) -> anyhow::Result<()> {
        VerifyStageLinear::execute(self).await
    }
}
