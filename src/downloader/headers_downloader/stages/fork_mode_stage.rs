use super::{
    headers::{
        header::BlockHeader, header_slice_status_watch::HeaderSliceStatusWatch, header_slices,
        header_slices::*,
    },
    verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{models::BlockNumber, sentry::chain_config::ChainConfig};
use ethereum_types::U256;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use std::{
    ops::{ControlFlow, DerefMut, Range},
    sync::Arc,
};
use tracing::*;

pub struct ForkModeStage {
    header_slices: Arc<HeaderSlices>,
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    canonical_range: Range<BlockNumber>,
    fork_range: Range<BlockNumber>,
    pending_watch: HeaderSliceStatusWatch,
    remaining_count: usize,
}

impl ForkModeStage {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    ) -> Self {
        let Some(fork_slice_lock) = header_slices.find_by_status(HeaderSliceStatus::Fork) else {
            panic!("invalid state: initial fork slice not found");
        };

        let canonical_range = Self::find_canonical_range(&header_slices);

        Self {
            header_slices: header_slices.clone(),
            chain_config,
            verifier,
            canonical_range,
            fork_range: fork_slice_lock.read().block_num_range(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::VerifiedInternally,
                header_slices,
                "ForkModeStage",
            ),
            remaining_count: 0,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        self.pending_watch.wait_while(self.remaining_count).await?;

        self.process_pending()?;

        self.remaining_count = self.pending_watch.pending_count();
        Ok(())
    }

    // initial setup: start refetching on both ends
    pub fn setup(&mut self) {
        let Some(canonical_continuation_slice_lock) = self.find_canonical_continuation_slice() else { return };
        let Some(fork_continuation_slice_lock) = self.find_fork_continuation_slice() else { return };

        let canonical_continuation_slice = canonical_continuation_slice_lock.upgradable_read();
        let fork_continuation_slice = fork_continuation_slice_lock.upgradable_read();

        if (self.fork_range.start == self.canonical_range.end)
            && (canonical_continuation_slice.status == HeaderSliceStatus::Fork)
            && (Self::is_canonical_slice_status(fork_continuation_slice.status))
        {
            let mut canonical_continuation_slice_mut =
                RwLockUpgradableReadGuard::upgrade(canonical_continuation_slice);
            let mut fork_continuation_slice_mut =
                RwLockUpgradableReadGuard::upgrade(fork_continuation_slice);
            self.refetch_slice(canonical_continuation_slice_mut.deref_mut());
            self.refetch_slice(fork_continuation_slice_mut.deref_mut());
        }
    }

    fn process_pending(&mut self) -> anyhow::Result<()> {
        let Some(canonical_continuation_slice_lock) = self.find_canonical_continuation_slice() else { return Ok(()) };
        let Some(fork_continuation_slice_lock) = self.find_fork_continuation_slice() else { return Ok(()) };

        // try to extend the chains
        let mut did_extend_canonical = false;
        let mut did_extend_fork = false;

        if canonical_continuation_slice_lock.read().status == HeaderSliceStatus::VerifiedInternally
        {
            let continuation_slice_lock = canonical_continuation_slice_lock;
            did_extend_canonical = self.try_extend_canonical(continuation_slice_lock.clone());
            if !did_extend_canonical {
                self.refetch_slice(continuation_slice_lock.write().deref_mut());
            }
        }

        if fork_continuation_slice_lock.read().status == HeaderSliceStatus::VerifiedInternally {
            let continuation_slice_lock = fork_continuation_slice_lock;
            did_extend_fork = self.try_extend_fork(continuation_slice_lock.clone());
            if !did_extend_fork {
                self.refetch_slice(continuation_slice_lock.write().deref_mut());
            }
        }

        // check termination conditions
        if did_extend_fork {
            let fork_first_slice_lock = self.find_fork_first_slice().unwrap();
            let connection_block_num_opt =
                Self::find_fork_connection_block_num(&fork_first_slice_lock.read());
            if let Some(connection_block_num) = connection_block_num_opt {
                if self.fork_range_difficulty(connection_block_num)
                    > self.canonical_range_difficulty(connection_block_num)
                {
                    self.switch_to_fork(connection_block_num);
                } else {
                    self.discard_fork();
                }
                return Ok(());
            }

            if self.fork_range.start == self.canonical_range.start {
                self.discard_fork();
                return Ok(());
            }
        }

        // if not terminated, continue refetching

        if did_extend_canonical {
            if let Some(continuation_slice_lock) = self.find_canonical_continuation_slice() {
                self.refetch_slice(continuation_slice_lock.write().deref_mut());
            }
        }

        if did_extend_fork {
            if let Some(continuation_slice_lock) = self.find_fork_continuation_slice() {
                self.refetch_slice(continuation_slice_lock.write().deref_mut());
            }
        }

        Ok(())
    }

    pub fn is_done(&self) -> bool {
        self.fork_range.is_empty()
    }

    fn refetch_slice(&self, slice: &mut HeaderSlice) {
        if slice.fork_headers.is_none() {
            slice.fork_status = slice.status;
            slice.fork_headers = slice.headers.take();
        }
        self.header_slices
            .set_slice_status(slice, HeaderSliceStatus::Refetch);
        slice.headers = None;
    }

    fn try_extend_canonical(&mut self, continuation_slice_lock: Arc<RwLock<HeaderSlice>>) -> bool {
        let Some(end_slice_lock) = self.find_canonical_last_slice() else { return false };
        let end_slice = end_slice_lock.read();
        let continuation_slice = continuation_slice_lock.upgradable_read();

        if self.verify_canonical_slices_link(&continuation_slice, &end_slice) {
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
        let end_slice = end_slice_lock.read();
        let continuation_slice = continuation_slice_lock.upgradable_read();

        if self.verify_fork_slices_link(&end_slice, &continuation_slice) {
            let mut continuation_slice_mut = RwLockUpgradableReadGuard::upgrade(continuation_slice);
            let continuation_slice = continuation_slice_mut.deref_mut();

            self.header_slices
                .set_slice_status(continuation_slice, HeaderSliceStatus::Fork);
            continuation_slice.refetch_attempt = 0;
            self.fork_range.start = continuation_slice.block_num_range().start;
            true
        } else {
            false
        }
    }

    fn verify_fork_slices_link(
        &self,
        child_slice: &HeaderSlice,
        parent_slice: &HeaderSlice,
    ) -> bool {
        let child_headers_opt = if child_slice.status == HeaderSliceStatus::Fork {
            child_slice.headers.as_ref()
        } else if child_slice.fork_status == HeaderSliceStatus::Fork {
            child_slice.fork_headers.as_ref()
        } else {
            None
        };

        let Some(child_headers) = child_headers_opt else { return false; };
        let Some(parent_headers) = &parent_slice.headers else { return false; };

        self.verify_headers_link(child_headers, parent_headers)
    }

    fn verify_canonical_slices_link(
        &self,
        child_slice: &HeaderSlice,
        parent_slice: &HeaderSlice,
    ) -> bool {
        let parent_headers_opt = if Self::is_canonical_slice_status(parent_slice.status) {
            parent_slice.headers.as_ref()
        } else if Self::is_canonical_slice_status(parent_slice.fork_status) {
            parent_slice.fork_headers.as_ref()
        } else {
            None
        };

        let Some(child_headers) = &child_slice.headers else { return false; };
        let Some(parent_headers) = &parent_headers_opt else { return false; };

        self.verify_headers_link(child_headers, parent_headers)
    }

    fn is_canonical_slice_status(status: HeaderSliceStatus) -> bool {
        (status == HeaderSliceStatus::Verified) || (status == HeaderSliceStatus::Saved)
    }

    fn is_fork_slice_status(status: HeaderSliceStatus) -> bool {
        status == HeaderSliceStatus::Fork
    }

    fn verify_headers_link(
        &self,
        child_headers: &[BlockHeader],
        parent_headers: &[BlockHeader],
    ) -> bool {
        let child = child_headers.first().unwrap();
        let parent = parent_headers.last().unwrap();
        self.verifier
            .verify_link(child, parent, self.chain_config.chain_spec())
    }

    fn find_fork_connection_block_num(fork_slice: &HeaderSlice) -> Option<BlockNumber> {
        let Some(headers) = &fork_slice.headers else { return None; };
        let Some(fork_headers) = &fork_slice.fork_headers else { return None; };
        headers
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

            match slice.status {
                HeaderSliceStatus::Verified | HeaderSliceStatus::Saved => {
                    range.end = slice.block_num_range().end;
                    ControlFlow::Continue(())
                }
                _ => ControlFlow::Break(()),
            }
        });

        range
    }

    /// Calculate total difficulty of the canonical chain starting after the connection block.
    fn canonical_range_difficulty(&self, connection_block_num: BlockNumber) -> U256 {
        self.range_difficulty(
            self.canonical_range.clone(),
            connection_block_num,
            Self::is_canonical_slice_status,
        )
    }

    /// Calculate total difficulty of the fork starting after the connection block.
    fn fork_range_difficulty(&self, connection_block_num: BlockNumber) -> U256 {
        self.range_difficulty(
            self.fork_range.clone(),
            connection_block_num,
            Self::is_fork_slice_status,
        )
    }

    /// Calculate total difficulty of the range starting after the connection block.
    fn range_difficulty(
        &self,
        range: Range<BlockNumber>,
        connection_block_num: BlockNumber,
        slice_status_predicate: impl Fn(HeaderSliceStatus) -> bool,
    ) -> U256 {
        let mut difficulty: U256 = U256::zero();

        for num in range {
            if num <= connection_block_num {
                continue;
            }

            let Some(slice_lock) = self.header_slices.find_by_block_num(num) else {
                warn!("range_difficulty invalid state: slice not found");
                break;
            };
            let slice = slice_lock.read();

            let slice_headers_opt = if slice_status_predicate(slice.status) {
                slice.headers.as_ref()
            } else if slice_status_predicate(slice.fork_status) {
                slice.fork_headers.as_ref()
            } else {
                None
            };
            let Some(slice_headers) = slice_headers_opt else {
                warn!("range_difficulty invalid state: slice headers not present");
                break;
            };

            let index = (num.0 - slice.start_block_num.0) as usize;
            let header = &slice_headers[index];
            difficulty += header.difficulty()
        }

        difficulty
    }

    fn switch_to_fork(&mut self, connection_block_num: BlockNumber) {
        // stop refetching
        while let Some(slice_lock) = self
            .header_slices
            .find_by_status(HeaderSliceStatus::Refetch)
        {
            let mut slice = slice_lock.write();
            if slice.status == HeaderSliceStatus::Refetch {
                let new_status = slice.fork_status;
                self.header_slices
                    .set_slice_status(slice.deref_mut(), new_status);
                slice.headers = slice.fork_headers.take();
                slice.fork_status = HeaderSliceStatus::Empty;
                slice.refetch_attempt = 0;
            }
        }

        // swap fork headers before the connection point to existing canonical
        // (although in practice they should also be equal)
        {
            let fork_first_slice_lock = self.find_fork_first_slice().unwrap();
            let mut fork_first_slice = fork_first_slice_lock.write();
            let len = (connection_block_num.0 - fork_first_slice.start_block_num.0) as usize;
            let canonical_headers = fork_first_slice.fork_headers.take().unwrap();
            let fork_headers = fork_first_slice.headers.as_mut().unwrap();
            let fork_headers_part = &mut fork_headers[0..len];
            fork_headers_part.clone_from_slice(&canonical_headers[0..len]);
        }

        // promote the fork chain
        let mut num = self.fork_range.start;
        while num < self.fork_range.end {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            // promote the status
            if slice.status == HeaderSliceStatus::Fork {
                self.header_slices
                    .set_slice_status(slice, HeaderSliceStatus::Verified);
            } else if slice.fork_status == HeaderSliceStatus::Fork {
                self.header_slices
                    .set_slice_status(slice, HeaderSliceStatus::Verified);
                slice.headers = slice.fork_headers.take();
            } else {
                panic!(
                    "switch_to_fork invalid state: fork slice has an unexpected status {:?}/{:?}",
                    slice.status, slice.fork_status
                );
            }

            // cleanup the fork data
            slice.fork_status = HeaderSliceStatus::Empty;
            slice.fork_headers = None;
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

            self.header_slices
                .set_slice_status(slice, HeaderSliceStatus::Empty);
            slice.headers = None;

            // cleanup the fork data
            slice.fork_status = HeaderSliceStatus::Empty;
            slice.fork_headers = None;
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + len as u64);
        }

        // done
        self.canonical_range.end = self.fork_range.end;
        self.fork_range.start = self.fork_range.end;
    }

    fn discard_fork(&mut self) {
        let mut num = self.fork_range.start;

        // add a potential fork continuation slice to the range
        if self.canonical_range.start < num {
            num = align_block_num_to_slice_start(BlockNumber(num.0 - 1));
        }

        let last_fork_slice_start =
            align_block_num_to_slice_start(BlockNumber(self.fork_range.end.0 - 1));

        while num < last_fork_slice_start {
            let slice_lock = self.header_slices.find_by_start_block_num(num).unwrap();
            let mut slice_mut = slice_lock.write();
            let slice = slice_mut.deref_mut();

            // slices within the fork range before the last must have a canonical fork_status
            // (including the fork continuation slice if any)
            assert!(Self::is_canonical_slice_status(slice.fork_status));
            let len = slice.fork_len();
            assert!(len > 0, "a canonical chain slice must have headers");

            // recover the backed up data
            self.header_slices
                .set_slice_status(slice, slice.fork_status);
            slice.headers = slice.fork_headers.take();

            // cleanup the fork data
            slice.fork_status = HeaderSliceStatus::Empty;
            slice.fork_headers = None;
            slice.refetch_attempt = 0;

            num = BlockNumber(num.0 + len as u64);
        }

        // the last fork slice has X/Y status (typically +/Y)
        let last_fork_slice_lock = self
            .header_slices
            .find_by_start_block_num(last_fork_slice_start)
            .unwrap();
        let mut last_fork_slice = last_fork_slice_lock.write();

        // cleanup the fork data
        last_fork_slice.fork_status = HeaderSliceStatus::Empty;
        last_fork_slice.fork_headers = None;
        last_fork_slice.refetch_attempt = 0;

        // done
        self.fork_range.start = self.fork_range.end;
    }

    fn find_canonical_continuation_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.header_slices
            .find_by_block_num(self.canonical_range.end)
    }

    fn find_fork_continuation_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.header_slices
            .find_by_block_num(BlockNumber(self.fork_range.start.0 - 1))
    }

    fn find_canonical_last_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.header_slices
            .find_by_block_num(BlockNumber(self.canonical_range.end.0 - 1))
    }

    fn find_fork_first_slice(&self) -> Option<Arc<RwLock<HeaderSlice>>> {
        self.header_slices.find_by_block_num(self.fork_range.start)
    }

    pub fn can_proceed_check(&self) -> impl Fn() -> bool {
        let header_slices = self.header_slices.clone();
        move || -> bool { header_slices.contains_status(HeaderSliceStatus::VerifiedInternally) }
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
