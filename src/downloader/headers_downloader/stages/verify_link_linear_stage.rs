use super::{
    headers::{
        header::BlockHeader,
        header_slice_status_watch::HeaderSliceStatusWatch,
        header_slices,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{models::BlockNumber, sentry::chain_config::ChainConfig};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
    ops::{ControlFlow, DerefMut},
    sync::Arc,
};
use tracing::*;

/// Verifies the sequence rules to link the slices with the last known verified header and sets Verified status.
pub struct VerifyLinkLinearStage {
    header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    start_block_num: BlockNumber,
    start_block_hash: ethereum_types::H256,
    invalid_status: HeaderSliceStatus,
    last_verified_header: Option<BlockHeader>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: usize,
}

impl VerifyLinkLinearStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
        start_block_num: BlockNumber,
        start_block_hash: ethereum_types::H256,
        invalid_status: HeaderSliceStatus,
    ) -> Self {
        let last_verified_header = Self::find_last_verified_header(&header_slices);

        Self {
            header_slices: header_slices.clone(),
            chain_config,
            verifier,
            start_block_num,
            start_block_hash,
            invalid_status,
            last_verified_header,
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::VerifiedInternally,
                header_slices,
                "VerifyLinkLinearStage",
            ),
            remaining_count: 0,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        // initially remaining_count = 0, so we wait for any internally verified slices to try to link them
        // since we want to link sequentially, there might be some remaining slices
        // in this case we wait until some more slices become internally verified
        // hopefully its the slices at the front so that we can link them too
        self.pending_watch.wait_while(self.remaining_count).await?;

        let pending_count = self.pending_watch.pending_count();

        debug!("VerifyLinkLinearStage: verifying {} slices", pending_count);
        let updated_count = self.verify_pending_monotonic(pending_count)?;
        debug!("VerifyLinkLinearStage: updated {} slices", updated_count);

        self.remaining_count = pending_count - updated_count;

        Ok(())
    }

    fn verify_pending_monotonic(&mut self, pending_count: usize) -> anyhow::Result<usize> {
        let mut updated_count: usize = 0;
        for _ in 0..pending_count {
            let next_slice_lock = self.find_next_pending_monotonic();

            if let Some(slice_lock) = next_slice_lock {
                let is_verified = self.verify_pending_slice(slice_lock);
                updated_count += 1;
                if !is_verified {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(updated_count)
    }

    fn find_next_pending_monotonic(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        let initial_value = Option::<Arc<RwLock<HeaderSlice>>>::None;
        let next_slice_lock = self.header_slices.try_fold(initial_value, |_, slice_lock| {
            let slice = slice_lock.read();
            match slice.status {
                HeaderSliceStatus::Verified | HeaderSliceStatus::Saved => {
                    ControlFlow::Continue(None)
                }
                HeaderSliceStatus::VerifiedInternally => {
                    ControlFlow::Break(Some(slice_lock.clone()))
                }
                _ => ControlFlow::Break(None),
            }
        });

        if let ControlFlow::Break(slice_lock_opt) = next_slice_lock {
            slice_lock_opt
        } else {
            None
        }
    }

    fn find_last_verified_header(header_slices: &HeaderSlices) -> Option<BlockHeader> {
        let mut last_verified_slice = Option::<Arc<RwLock<HeaderSlice>>>::None;

        header_slices.try_fold((), |_, slice_lock| {
            let slice = slice_lock.read();
            match slice.status {
                HeaderSliceStatus::Verified | HeaderSliceStatus::Saved => {
                    last_verified_slice = Some(slice_lock.clone());
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(()),
            }
        });

        last_verified_slice.and_then(|slice_lock| {
            let slice = slice_lock.read();
            let headers = slice.headers.as_ref();
            headers.and_then(|headers| -> Option<BlockHeader> { headers.last().cloned() })
        })
    }

    fn verify_pending_slice(&mut self, slice_lock: Arc<RwLock<HeaderSlice>>) -> bool {
        let slice = slice_lock.upgradable_read();

        let is_verified = self.verify_slice_link(&slice, &self.last_verified_header);

        let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
        if is_verified {
            self.header_slices
                .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Verified);
            if let Some(last_verified_header) = slice.headers.as_ref().unwrap().iter().last() {
                self.last_verified_header = Some(last_verified_header.clone());
            }
        } else {
            self.header_slices
                .set_slice_status(slice.deref_mut(), self.invalid_status);
        }

        is_verified
    }

    fn verify_slice_link(&self, slice: &HeaderSlice, parent: &Option<BlockHeader>) -> bool {
        let Some(headers) = slice.headers.as_ref() else {
            return false;
        };

        if headers.is_empty() {
            return false;
        }
        if headers.len() != header_slices::HEADER_SLICE_SIZE {
            return false;
        }

        let child = &headers[0];

        // for the start header we just verify its hash
        if child.number() == self.start_block_num {
            return child.hash() == self.start_block_hash;
        }
        // otherwise we expect that we have a verified parent
        if parent.is_none() {
            return false;
        }
        let parent = parent.as_ref().unwrap();

        self.verifier
            .verify_link(child, parent, self.chain_config.chain_spec())
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        move || -> bool { header_slices.contains_status(HeaderSliceStatus::VerifiedInternally) }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyLinkLinearStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
