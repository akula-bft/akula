use super::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slice_verifier,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
};
use crate::sentry::chain_config::ChainConfig;
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc, time::SystemTime};
use tracing::*;

/// Verifies the block structure and sequence rules in each slice and sets VerifiedInternally status.
pub struct VerifyStageLinear {
    header_slices: Arc<HeaderSlices>,
    slice_size: usize,
    chain_config: ChainConfig,
    pending_watch: HeaderSliceStatusWatch,
}

impl VerifyStageLinear {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        slice_size: usize,
        chain_config: ChainConfig,
    ) -> Self {
        Self {
            header_slices: header_slices.clone(),
            slice_size,
            chain_config,
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
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::VerifiedInternally);
                } else {
                    self.header_slices
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Invalid);
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
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();
        if headers.len() != self.slice_size {
            return false;
        }

        header_slice_verifier::verify_slice_is_linked_by_parent_hash(headers)
            && header_slice_verifier::verify_slice_block_nums(headers, slice.start_block_num)
            && header_slice_verifier::verify_slice_timestamps(headers, Self::now_timestamp())
            && header_slice_verifier::verify_slice_difficulties(
                headers,
                self.chain_config.chain_spec(),
            )
            && header_slice_verifier::verify_slice_pow(headers)
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyStageLinear {
    async fn execute(&mut self) -> anyhow::Result<()> {
        VerifyStageLinear::execute(self).await
    }
}
