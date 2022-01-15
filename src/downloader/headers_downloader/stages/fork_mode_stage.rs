use super::{
    headers::{
        header_slice_status_watch::{HeaderSliceStatusWatch, HeaderSliceStatusWatchSelector},
        header_slices,
        header_slices::*,
    },
    verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{models::*, sentry::chain_config::ChainConfig};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
    ops::{ControlFlow, DerefMut, Range},
    sync::Arc,
};
use tracing::*;

pub struct ForkModeStage {
    header_slices: Arc<HeaderSlices>,
    fork_header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    canonical_range: Range<BlockNumber>,
    fork_range: Range<BlockNumber>,
    pending_watch: HeaderSliceStatusWatchSelector,
    remaining_count: usize,
    fork_remaining_count: usize,
}

impl ForkModeStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        fork_header_slices: Arc<HeaderSlices>,
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    ) -> Self {
        let canonical_range = Self::find_canonical_range(&header_slices);
        let fork_range = Self::find_fork_range(&fork_header_slices);

        let pending_watch = HeaderSliceStatusWatchSelector::new(
            "ForkModeStage",
            HeaderSliceStatusWatch::new(
                HeaderSliceStatus::VerifiedInternally,
                header_slices.clone(),
                "ForkModeStage",
            ),
            HeaderSliceStatusWatch::new(
                HeaderSliceStatus::VerifiedInternally,
                fork_header_slices.clone(),
                "ForkModeStage",
            ),
        );

        Self {
            header_slices,
            fork_header_slices,
            chain_config,
            verifier,
            canonical_range,
            fork_range,
            pending_watch,
            remaining_count: 0,
            fork_remaining_count: 0,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.pending_watch
            .wait_while_all((self.remaining_count, self.fork_remaining_count))
            .await?;
        let (pending_count, fork_pending_count) = self.pending_watch.pending_counts();

        debug!(
            "ForkModeStage: processing {} slices and {} fork slices",
            pending_count, fork_pending_count
        );
        let (updated_count, fork_updated_count) =
            self.process_pending(pending_count, fork_pending_count)?;

        self.remaining_count = pending_count - updated_count;
        self.fork_remaining_count = fork_pending_count - fork_updated_count;
        Ok(())
    }

    // initial setup: start refetching on both ends
    pub fn setup(&mut self) -> anyhow::Result<()> {
        let Some(canonical_continuation_slice_lock) = self.find_canonical_continuation_slice() else {
            return Err(anyhow::format_err!("ForkModeStage.setup: initial fork slice not found"));
        };

        let mut canonical_continuation_slice = canonical_continuation_slice_lock.write();
        if canonical_continuation_slice.status != HeaderSliceStatus::Fork {
            return Err(anyhow::format_err!(
                "ForkModeStage.setup: initial fork slice invalid status"
            ));
        }

        let fork_start = canonical_continuation_slice.start_block_num;
        let fork_initial_slice_headers = canonical_continuation_slice.headers.take();
        let fork_initial_slice_peer = canonical_continuation_slice.from_peer_id;
        self.refetch_canonical_slice(canonical_continuation_slice.deref_mut());

        let fork_initial_slice = HeaderSlice {
            start_block_num: fork_start,
            status: HeaderSliceStatus::Verified,
            headers: fork_initial_slice_headers,
            from_peer_id: fork_initial_slice_peer,
            ..Default::default()
        };
        let fork_range = fork_initial_slice.block_num_range();

        self.fork_header_slices
            .reset_to_single_slice(fork_initial_slice);
        self.fork_range = fork_range;
        self.fork_header_slices.prepend_slice()?;
        Ok(())
    }

    fn process_pending(
        &mut self,
        pending_count: usize,
        fork_pending_count: usize,
    ) -> anyhow::Result<(usize, usize)> {
        // try to extend the chains
        let mut did_extend_canonical = false;
        let mut did_extend_fork = false;

        let mut updated_count = 0;
        let mut fork_updated_count = 0;

        // try to extend the canonical chain
        let canonical_continuation_slice_lock_opt = self.find_canonical_continuation_slice();
        let canonical_continuation_slice_status = canonical_continuation_slice_lock_opt
            .as_ref()
            .map(|slice| slice.read().status);
        if (canonical_continuation_slice_status == Some(HeaderSliceStatus::VerifiedInternally))
            && (updated_count < pending_count)
        {
            let continuation_slice_lock = canonical_continuation_slice_lock_opt.unwrap();
            did_extend_canonical = self.try_extend_canonical(continuation_slice_lock.clone());
            if !did_extend_canonical {
                self.refetch_canonical_slice(continuation_slice_lock.write().deref_mut());
            }
            updated_count += 1;
        }

        // try to extend the fork chain
        let fork_continuation_slice_lock_opt = self.find_fork_continuation_slice();
        let fork_continuation_slice_status = fork_continuation_slice_lock_opt
            .as_ref()
            .map(|slice| slice.read().status);
        if (fork_continuation_slice_status == Some(HeaderSliceStatus::VerifiedInternally))
            && (fork_updated_count < fork_pending_count)
        {
            let continuation_slice_lock = fork_continuation_slice_lock_opt.unwrap();
            did_extend_fork = self.try_extend_fork(continuation_slice_lock.clone());
            if !did_extend_fork {
                self.refetch_fork_slice(continuation_slice_lock.write().deref_mut());
            }
            fork_updated_count += 1;
        }

        // check termination conditions
        if did_extend_fork {
            let connection_block_num_opt = self.find_fork_connection_block_num();
            if let Some(connection_block_num) = connection_block_num_opt {
                let fork_range_difficulty = self.fork_range_difficulty(connection_block_num);
                let canonical_range_difficulty =
                    self.canonical_range_difficulty(connection_block_num);
                if fork_range_difficulty > canonical_range_difficulty {
                    self.switch_to_fork();
                } else {
                    self.discard_fork();
                }
                return Ok((updated_count, fork_updated_count));
            }

            if self.fork_range.start == self.canonical_range.start {
                self.discard_fork();
                return Ok((updated_count, fork_updated_count));
            }
        }

        // if not terminated, continue loading adjacent slices further

        // continue prolonging the canonical chain
        if did_extend_canonical {
            if let Some(continuation_slice_lock) = self.find_canonical_continuation_slice() {
                let old_status = continuation_slice_lock.read().status;
                self.refetch_canonical_slice(continuation_slice_lock.write().deref_mut());

                if (old_status == HeaderSliceStatus::VerifiedInternally)
                    && (updated_count < pending_count)
                {
                    updated_count += 1;
                }
            }
        }

        // continue prolonging the fork chain
        if did_extend_fork {
            self.fork_header_slices.prepend_slice()?;
        }

        Ok((updated_count, fork_updated_count))
    }

    pub fn is_done(&self) -> bool {
        self.fork_header_slices.is_empty()
    }

    fn refetch_canonical_slice(&self, slice: &mut HeaderSlice) {
        self.header_slices
            .set_slice_status(slice, HeaderSliceStatus::Refetch);
        slice.headers = None;
    }

    fn refetch_fork_slice(&self, slice: &mut HeaderSlice) {
        self.fork_header_slices
            .set_slice_status(slice, HeaderSliceStatus::Refetch);
        slice.headers = None;
    }

    fn try_extend_canonical(&mut self, continuation_slice_lock: Arc<RwLock<HeaderSlice>>) -> bool {
        let Some(end_slice_lock) = self.find_canonical_last_slice() else { return false };
        let end_slice = end_slice_lock.read();
        let continuation_slice = continuation_slice_lock.upgradable_read();

        if self.verify_slices_link(&continuation_slice, &end_slice) {
            let mut continuation_slice_mut = RwLockUpgradableReadGuard::upgrade(continuation_slice);
            let continuation_slice = continuation_slice_mut.deref_mut();

            self.header_slices
                .set_slice_status(continuation_slice, HeaderSliceStatus::Verified);
            continuation_slice.refetch_attempt = 0;
            self.canonical_range.end = continuation_slice.block_num_range().end;
            true
        } else {
            false
        }
    }

    fn try_extend_fork(&mut self, continuation_slice_lock: Arc<RwLock<HeaderSlice>>) -> bool {
        let Some(end_slice_lock) = self.find_fork_first_slice() else { return false };
        let continuation_slice = continuation_slice_lock.upgradable_read();
        let end_slice = end_slice_lock.read();

        if self.verify_slices_link(&end_slice, &continuation_slice) {
            let mut continuation_slice_mut = RwLockUpgradableReadGuard::upgrade(continuation_slice);
            let continuation_slice = continuation_slice_mut.deref_mut();

            self.fork_header_slices
                .set_slice_status(continuation_slice, HeaderSliceStatus::Verified);
            continuation_slice.refetch_attempt = 0;
            self.fork_range.start = continuation_slice.block_num_range().start;
            true
        } else {
            false
        }
    }

    fn verify_slices_link(&self, child_slice: &HeaderSlice, parent_slice: &HeaderSlice) -> bool {
        let Some(child_headers) = &child_slice.headers else { return false; };
        let Some(parent_headers) = &parent_slice.headers else { return false; };

        let Some(child) = child_headers.first() else { return false };
        let Some(parent) = parent_headers.last() else { return false };

        self.verifier
            .verify_link(child, parent, self.chain_config.chain_spec())
    }

    fn find_fork_connection_block_num(&self) -> Option<BlockNumber> {
        let Some(fork_slice_lock) = self.find_fork_first_slice() else { return None };
        let fork_slice = fork_slice_lock.read();

        let Some(canonical_slice_lock) = self.header_slices.find_by_start_block_num(fork_slice.start_block_num) else { return None };
        let canonical_slice = canonical_slice_lock.read();

        let Some(canonical_headers) = &canonical_slice.headers else { return None; };
        let Some(fork_headers) = &fork_slice.headers else { return None; };

        canonical_headers
            .iter()
            .zip(fork_headers.iter())
            .rfind(|(header, fork_header)| header.hash() == fork_header.hash())
            .map(|(header, _)| header.number())
    }

    fn find_canonical_range(header_slices: &HeaderSlices) -> Range<BlockNumber> {
        let mut range = BlockNumber(0)..BlockNumber(0);

        header_slices.try_fold((), |_, slice_lock| {
            let slice = slice_lock.read();

            if (range.start.0 == 0) && range.is_empty() {
                range.start = slice.start_block_num;
                range.end = slice.start_block_num;
            }

            if Self::is_canonical_slice_status(slice.status) {
                range.end = slice.block_num_range().end;
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });

        range
    }

    fn find_fork_range(header_slices: &HeaderSlices) -> Range<BlockNumber> {
        let mut range = BlockNumber(0)..BlockNumber(0);

        header_slices.try_rfold((), |_, slice_lock| {
            let slice = slice_lock.read();

            if (range.start.0 == 0) && range.is_empty() {
                range.end = slice.block_num_range().end;
                range.start = range.end;
            }

            if Self::is_canonical_slice_status(slice.status) {
                range.start = slice.start_block_num;
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });

        range
    }

    /// Calculate total difficulty of the canonical chain starting after the connection block.
    fn canonical_range_difficulty(&self, connection_block_num: BlockNumber) -> U256 {
        Self::range_difficulty(
            self.header_slices.clone(),
            self.canonical_range.clone(),
            connection_block_num,
        )
    }

    /// Calculate total difficulty of the fork starting after the connection block.
    fn fork_range_difficulty(&self, connection_block_num: BlockNumber) -> U256 {
        Self::range_difficulty(
            self.fork_header_slices.clone(),
            self.fork_range.clone(),
            connection_block_num,
        )
    }

    /// Calculate total difficulty of the range starting after the connection block.
    fn range_difficulty(
        header_slices: Arc<HeaderSlices>,
        range: Range<BlockNumber>,
        connection_block_num: BlockNumber,
    ) -> U256 {
        let mut difficulty = U256::ZERO;
        let mut num = BlockNumber(connection_block_num.0 + 1);

        while range.contains(&num) {
            let Some(slice_lock) = header_slices.find_by_block_num(num) else {
                warn!("range_difficulty invalid state: slice not found");
                break;
            };
            let slice = slice_lock.read();

            if !Self::is_canonical_slice_status(slice.status) {
                warn!("range_difficulty invalid state: bad status");
                break;
            }

            let Some(slice_headers) = slice.headers.as_ref() else {
                warn!("range_difficulty invalid state: slice headers not present");
                break;
            };

            let mut index = (num.0 - slice.start_block_num.0) as usize;

            while (index < slice_headers.len()) && range.contains(&num) {
                let header = &slice_headers[index];
                difficulty += header.difficulty();

                index += 1;
                num = BlockNumber(num.0 + 1);
            }
        }

        difficulty
    }

    fn switch_to_fork(&mut self) {
        // promote the fork chain
        let mut num = self.fork_range.start;
        while num < self.fork_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let fork_slice_lock = self
                .fork_header_slices
                .find_by_start_block_num(num)
                .unwrap();

            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            let mut fork_slice_mut = fork_slice_lock.write();
            let fork_slice = fork_slice_mut.deref_mut();

            // promote the status
            self.header_slices
                .set_slice_status(slice, fork_slice.status);
            slice.headers = fork_slice.headers.take();
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + slice.len() as u64);
        }

        // adjust num to point to the first canonical slice after the fork
        // in case if the last fork slice is partial
        num = align_block_num_to_slice_start(BlockNumber(
            num.0 + (header_slices::HEADER_SLICE_SIZE as u64) - 1,
        ));

        // discard the canonical chain after the fork
        while num < self.canonical_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            // slices within the canonical range past the fork must be in a canonical status
            assert!(Self::is_canonical_slice_status(slice.status));
            let len = slice.len();
            assert!(len > 0, "a canonical chain slice must have headers");

            // reset the status
            self.header_slices
                .set_slice_status(slice, HeaderSliceStatus::Empty);
            slice.headers = None;
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + len as u64);
        }

        // done
        self.canonical_range.end = self.fork_range.end;
        self.discard_fork();
    }

    fn discard_fork(&mut self) {
        self.fork_header_slices.clear();
        self.fork_range.start = self.fork_range.end;
    }

    fn find_canonical_continuation_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.header_slices
            .find_by_block_num(self.canonical_range.end)
    }

    fn find_fork_continuation_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        let fork_start = self.fork_range.start.0;
        if fork_start == 0 {
            return None;
        }
        self.fork_header_slices
            .find_by_block_num(BlockNumber(fork_start - 1))
    }

    fn find_canonical_last_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        let canonical_end = self.canonical_range.end.0;
        if canonical_end == 0 {
            return None;
        }
        self.header_slices
            .find_by_block_num(BlockNumber(canonical_end - 1))
    }

    fn find_fork_first_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.fork_header_slices
            .find_by_block_num(self.fork_range.start)
    }

    fn is_canonical_slice_status(status: HeaderSliceStatus) -> bool {
        (status == HeaderSliceStatus::Verified) || (status == HeaderSliceStatus::Saved)
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        let fork_header_slices = self.fork_header_slices.clone();
        move || -> bool {
            header_slices.contains_status(HeaderSliceStatus::VerifiedInternally)
                || fork_header_slices.contains_status(HeaderSliceStatus::VerifiedInternally)
        }
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for ForkModeStage {
    async fn execute(&mut self) -> anyhow::Result<()> {
        Self::execute(self).await
    }
    fn can_proceed_check(&self) -> Box<dyn Fn() -> bool + Send> {
        Box::new(Self::can_proceed_check(self))
    }
}
