use super::header::BlockHeader;
use crate::{models::BlockNumber, sentry_connector::sentry_client::PeerId};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time,
};
use strum::IntoEnumIterator;
use tokio::sync::watch;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, strum::EnumIter, strum::Display)]
pub enum HeaderSliceStatus {
    // initialized, needs to be obtained
    Empty,
    // fetch request sent to sentry
    Waiting,
    // received from sentry
    Downloaded,
    // headers inside the slice have a consistent structure, and linked in a proper way
    VerifiedInternally,
    // headers of the slice and linked in a proper way to a known verified header
    Verified,
    // verification failed
    Invalid,
    // linking to the canonical chain failed, a potential fork
    Fork,
    // saved in the database
    Saved,
}

#[derive(Default, Clone)]
pub struct HeaderSlice {
    pub start_block_num: BlockNumber,
    pub status: HeaderSliceStatus,
    pub headers: Option<Vec<BlockHeader>>,
    pub from_peer_id: Option<PeerId>,
    pub request_time: Option<time::Instant>,
    pub request_attempt: u16,
    pub refetch_attempt: u16,
}

struct HeaderSliceStatusWatch {
    pub sender: watch::Sender<usize>,
    pub receiver: watch::Receiver<usize>,
    pub count: AtomicUsize,
}

/// The pre-verified headers are downloaded with a help of a HeaderSlices data structure.
/// HeaderSlices is a memory-limited buffer of HeaderSlice objects, each containing a sequential slice of headers.
///
/// Example of a HeaderSlices with max_slices = 3:
/// HeaderSlice 0: headers 0-192
/// HeaderSlice 1: headers 192-384
/// HeaderSlice 2: headers 384-576
pub struct HeaderSlices {
    slices: RwLock<VecDeque<Arc<RwLock<HeaderSlice>>>>,
    max_slices: usize,
    max_block_num: AtomicU64,
    final_block_num: BlockNumber,
    state_watches: HashMap<HeaderSliceStatus, HeaderSliceStatusWatch>,
}

pub const HEADER_SLICE_SIZE: usize = 192;

const ATOMIC_ORDERING: Ordering = Ordering::SeqCst;

impl HeaderSlices {
    pub fn new(
        mem_limit: usize,
        start_block_num: BlockNumber,
        final_block_num: BlockNumber,
    ) -> Self {
        let total_block_num = (final_block_num.0 - start_block_num.0) as usize;
        let max_slices = std::cmp::min(
            estimate_max_slices_for_mem_limit(mem_limit),
            (total_block_num + HEADER_SLICE_SIZE - 1) / HEADER_SLICE_SIZE,
        );

        Self::from_slices_vec(
            Vec::new(),
            Some(start_block_num),
            Some(max_slices),
            Some(final_block_num),
        )
    }

    #[allow(clippy::needless_range_loop)]
    pub fn from_slices_vec(
        mut slices: Vec<HeaderSlice>,
        start_block_num_opt: Option<BlockNumber>,
        max_slices_opt: Option<usize>,
        final_block_num_opt: Option<BlockNumber>,
    ) -> Self {
        let slices_count = slices.len();
        let start_block_num = if slices_count > 0 {
            slices[0].start_block_num
        } else {
            start_block_num_opt.unwrap_or(BlockNumber(0))
        };
        assert!(
            is_block_num_aligned_to_slice_start(start_block_num),
            "start_block_num must be at the slice boundary"
        );

        let max_slices = max_slices_opt.unwrap_or(slices_count);

        slices.resize(max_slices, Default::default());
        for i in slices_count..max_slices {
            let block_num = start_block_num.0 + (i * HEADER_SLICE_SIZE) as u64;
            slices[i].start_block_num = BlockNumber(block_num);
        }

        let max_block_num = start_block_num.0 + (max_slices * HEADER_SLICE_SIZE) as u64;

        let final_block_num = final_block_num_opt.unwrap_or(BlockNumber(max_block_num));
        assert!(
            is_block_num_aligned_to_slice_start(final_block_num),
            "final_block_num must be at the slice boundary"
        );

        let state_watches = Self::make_state_watches_from_slices(&slices);

        let slice_locks =
            VecDeque::from_iter(slices.into_iter().map(|slice| Arc::new(RwLock::new(slice))));

        Self {
            slices: RwLock::new(slice_locks),
            max_slices,
            max_block_num: AtomicU64::new(max_block_num),
            final_block_num,
            state_watches,
        }
    }

    pub fn empty(max_slices: usize) -> Self {
        Self {
            slices: RwLock::new(VecDeque::new()),
            max_slices,
            max_block_num: AtomicU64::new(0),
            final_block_num: BlockNumber(0),
            state_watches: Self::make_state_watches_from_slices(&[]),
        }
    }

    #[cfg(test)]
    pub fn clone_slices_vec(&self) -> Vec<HeaderSlice> {
        self.slices
            .read()
            .iter()
            .map(|slice_lock| slice_lock.read().clone())
            .collect()
    }

    fn make_state_watches_from_slices(
        slices: &[HeaderSlice],
    ) -> HashMap<HeaderSliceStatus, HeaderSliceStatusWatch> {
        let mut state_counters = HashMap::<HeaderSliceStatus, usize>::new();
        for id in HeaderSliceStatus::iter() {
            state_counters.insert(id, 0);
        }
        for slice in slices {
            state_counters.insert(slice.status, state_counters[&slice.status] + 1);
        }

        let mut state_watches = HashMap::<HeaderSliceStatus, HeaderSliceStatusWatch>::new();
        for (id, initial_count) in state_counters {
            let (sender, receiver) = watch::channel(initial_count);
            let channel = HeaderSliceStatusWatch {
                sender,
                receiver,
                count: AtomicUsize::new(initial_count),
            };

            state_watches.insert(id, channel);
        }
        state_watches
    }

    pub fn clone_statuses(&self) -> Vec<HeaderSliceStatus> {
        self.slices
            .read()
            .iter()
            .map(|slice| slice.read().status)
            .collect::<Vec<HeaderSliceStatus>>()
    }

    pub fn for_each<F>(&self, f: F)
    where
        F: FnMut(&Arc<RwLock<HeaderSlice>>),
    {
        self.slices.read().iter().for_each(f);
    }

    pub fn try_fold<B, C, F>(&self, init: C, f: F) -> std::ops::ControlFlow<B, C>
    where
        F: FnMut(C, &Arc<RwLock<HeaderSlice>>) -> std::ops::ControlFlow<B, C>,
    {
        self.slices.read().iter().try_fold(init, f)
    }

    pub fn try_rfold<B, C, F>(&self, init: C, f: F) -> std::ops::ControlFlow<B, C>
    where
        F: FnMut(C, &Arc<RwLock<HeaderSlice>>) -> std::ops::ControlFlow<B, C>,
    {
        self.slices.read().iter().try_rfold(init, f)
    }

    pub fn find_by_start_block_num(
        &self,
        start_block_num: BlockNumber,
    ) -> Option<Arc<RwLock<HeaderSlice>>> {
        let slices = self.slices.read();
        slices
            .iter()
            .find(|slice| slice.read().start_block_num == start_block_num)
            .map(Arc::clone)
    }

    pub fn find_by_block_num(&self, block_num: BlockNumber) -> Option<Arc<RwLock<HeaderSlice>>> {
        let start_block_num = align_block_num_to_slice_start(block_num);
        let Some(slice_lock) = self.find_by_start_block_num(start_block_num) else {
            return None;
        };

        let is_block_num_in_range: bool = {
            let slice = slice_lock.read();
            let block_num_range = slice.block_num_range();
            block_num_range.contains(&block_num)
        };

        if is_block_num_in_range {
            Some(slice_lock)
        } else {
            None
        }
    }

    pub fn find_by_status(&self, status: HeaderSliceStatus) -> Option<Arc<RwLock<HeaderSlice>>> {
        let slices = self.slices.read();
        slices
            .iter()
            .find(|slice| slice.read().status == status)
            .map(Arc::clone)
    }

    pub fn find_batch_by_status(
        &self,
        status: HeaderSliceStatus,
        batch_size: usize,
    ) -> Vec<Arc<RwLock<HeaderSlice>>> {
        let mut batch = Vec::new();
        let slices = self.slices.read();
        for slice_lock in slices.iter() {
            let slice = slice_lock.read();
            if slice.status == status {
                batch.push(slice_lock.clone());
                if batch.len() == batch_size {
                    break;
                }
            }
        }
        batch
    }

    pub fn remove(&self, status: HeaderSliceStatus) {
        let mut slices = self.slices.write();

        let mut cursor = 0;
        let mut count: usize = 0;

        while cursor < slices.len() {
            let current_status = slices[cursor].read().status;
            if current_status == status {
                slices.remove(cursor);
                count += 1;
            } else {
                cursor += 1;
            }
        }

        let status_watch = &self.state_watches[&status];
        status_watch.count.fetch_sub(count, ATOMIC_ORDERING);
    }

    pub fn refill(&self) {
        let mut slices = self.slices.write();
        let initial_len = slices.len();
        let mut count = 0;

        for _ in initial_len..self.max_slices {
            let max_block_num = self.max_block_num();
            if max_block_num >= self.final_block_num {
                break;
            }

            let slice = HeaderSlice {
                start_block_num: max_block_num,
                ..Default::default()
            };
            slices.push_back(Arc::new(RwLock::new(slice)));
            self.max_block_num
                .fetch_add(HEADER_SLICE_SIZE as u64, ATOMIC_ORDERING);
            count += 1;
        }

        let status_watch = &self.state_watches[&HeaderSliceStatus::Empty];
        status_watch.count.fetch_add(count, ATOMIC_ORDERING);
    }

    pub fn clear(&self) {
        let mut slices = self.slices.write();
        slices.clear();
        self.max_block_num.store(0, ATOMIC_ORDERING);

        for watch in self.state_watches.values() {
            watch.count.store(0, ATOMIC_ORDERING);
        }
    }

    pub fn reset_to_single_slice(&self, initial_slice: HeaderSlice) {
        let start_block_num = initial_slice.start_block_num;
        let status = initial_slice.status;

        let mut slices = self.slices.write();
        slices.clear();
        slices.push_back(Arc::new(RwLock::new(initial_slice)));

        self.max_block_num.store(
            start_block_num.0 + HEADER_SLICE_SIZE as u64,
            ATOMIC_ORDERING,
        );

        for watch in self.state_watches.values() {
            watch.count.store(0, ATOMIC_ORDERING);
        }
        let status_watch = &self.state_watches[&status];
        status_watch.count.store(1, ATOMIC_ORDERING);
    }

    pub fn prepend_slice(&self) -> anyhow::Result<()> {
        let mut slices = self.slices.write();

        if slices.len() >= self.max_slices {
            return Err(anyhow::format_err!(
                "can't prepend: max_slices limit reached"
            ));
        }

        let Some(first) = slices.iter().next() else {
            return Err(anyhow::format_err!("can't prepend if empty"));
        };

        let first_block_num = first.read().start_block_num;
        if first_block_num.0 < HEADER_SLICE_SIZE as u64 {
            return Err(anyhow::format_err!(
                "can't prepend before block {}",
                first_block_num.0
            ));
        }
        let start_block_num = BlockNumber(first_block_num.0 - HEADER_SLICE_SIZE as u64);

        let slice = HeaderSlice {
            start_block_num,
            ..Default::default()
        };
        slices.push_front(Arc::new(RwLock::new(slice)));

        let status_watch = &self.state_watches[&HeaderSliceStatus::Empty];
        status_watch.count.fetch_add(1, ATOMIC_ORDERING);
        Ok(())
    }

    pub fn append_slice(&self) -> anyhow::Result<()> {
        let mut slices = self.slices.write();
        let slice = HeaderSlice {
            start_block_num: self.max_block_num(),
            ..Default::default()
        };
        slices.push_back(Arc::new(RwLock::new(slice)));
        self.max_block_num
            .fetch_add(HEADER_SLICE_SIZE as u64, ATOMIC_ORDERING);

        let status_watch = &self.state_watches[&HeaderSliceStatus::Empty];
        status_watch.count.fetch_add(1, ATOMIC_ORDERING);
        Ok(())
    }

    pub fn trim_start_to_fit_max_slices(&self) {
        let mut slices = self.slices.write();
        while slices.len() > self.max_slices {
            let removed_slice_lock = slices.pop_front().unwrap();
            let removed_status = removed_slice_lock.read().status;
            let status_watch = &self.state_watches[&removed_status];
            status_watch.count.fetch_sub(1, ATOMIC_ORDERING);
        }
    }

    pub fn has_one_of_statuses(&self, statuses: &[HeaderSliceStatus]) -> bool {
        statuses
            .iter()
            .any(|status| self.count_slices_in_status(*status) > 0)
    }

    pub fn contains_status(&self, status: HeaderSliceStatus) -> bool {
        self.count_slices_in_status(status) > 0
    }

    pub fn set_slice_status(&self, slice: &mut HeaderSlice, status: HeaderSliceStatus) {
        let old_status = slice.status;
        if status == old_status {
            return;
        }

        slice.status = status;

        let old_status_watch = &self.state_watches[&old_status];
        let new_status_watch = &self.state_watches[&status];

        old_status_watch.count.fetch_sub(1, ATOMIC_ORDERING);
        new_status_watch.count.fetch_add(1, ATOMIC_ORDERING);
    }

    pub fn watch_status_changes(&self, status: HeaderSliceStatus) -> watch::Receiver<usize> {
        let status_watch = &self.state_watches[&status];
        status_watch.receiver.clone()
    }

    pub fn notify_status_watchers(&self) {
        for watch in self.state_watches.values() {
            let count = watch.count.load(ATOMIC_ORDERING);
            let _ = watch.sender.send(count);
        }
    }

    pub fn count_slices_in_status(&self, status: HeaderSliceStatus) -> usize {
        let status_watch = &self.state_watches[&status];
        status_watch.count.load(ATOMIC_ORDERING)
    }

    pub fn status_counters(&self) -> Vec<(HeaderSliceStatus, usize)> {
        let mut counters = Vec::<(HeaderSliceStatus, usize)>::new();
        for (status, watch) in &self.state_watches {
            let count = watch.count.load(ATOMIC_ORDERING);
            counters.push((*status, count));
        }
        counters
    }

    pub fn max_slices(&self) -> usize {
        self.max_slices
    }

    pub fn min_block_num(&self) -> BlockNumber {
        if let Some(first_slice) = self.slices.read().front() {
            return first_slice.read().start_block_num;
        }
        self.max_block_num()
    }

    pub fn max_block_num(&self) -> BlockNumber {
        BlockNumber(self.max_block_num.load(ATOMIC_ORDERING))
    }

    pub fn final_block_num(&self) -> BlockNumber {
        self.final_block_num
    }

    pub fn is_empty(&self) -> bool {
        self.slices.read().is_empty()
    }

    pub fn is_empty_at_final_position(&self) -> bool {
        (self.max_block_num() >= self.final_block_num) && self.slices.read().is_empty()
    }

    pub fn all_in_status(&self, status: HeaderSliceStatus) -> bool {
        self.count_slices_in_status(status) == self.slices.read().len()
    }
}

pub fn align_block_num_to_slice_start(num: BlockNumber) -> BlockNumber {
    let slice_size = HEADER_SLICE_SIZE as u64;
    BlockNumber(num.0 / slice_size * slice_size)
}

pub fn is_block_num_aligned_to_slice_start(num: BlockNumber) -> bool {
    num.0 % (HEADER_SLICE_SIZE as u64) == 0
}

fn estimate_max_slices_for_mem_limit(mem_limit: usize) -> usize {
    mem_limit / std::mem::size_of::<BlockHeader>() / HEADER_SLICE_SIZE
}

impl HeaderSlice {
    pub fn len(&self) -> usize {
        self.headers.as_ref().map_or(0, |headers| headers.len())
    }

    pub fn block_num_range(&self) -> std::ops::Range<BlockNumber> {
        let end = BlockNumber(self.start_block_num.0 + self.len() as u64);
        self.start_block_num..end
    }
}

impl Default for HeaderSliceStatus {
    fn default() -> Self {
        HeaderSliceStatus::Empty
    }
}

impl From<HeaderSliceStatus> for char {
    fn from(status: HeaderSliceStatus) -> Self {
        match status {
            HeaderSliceStatus::Empty => '-',
            HeaderSliceStatus::Waiting => '<',
            HeaderSliceStatus::Downloaded => '.',
            HeaderSliceStatus::VerifiedInternally => '=',
            HeaderSliceStatus::Verified => '#',
            HeaderSliceStatus::Invalid => 'x',
            HeaderSliceStatus::Fork => 'Y',
            HeaderSliceStatus::Saved => '+',
        }
    }
}

impl TryFrom<char> for HeaderSliceStatus {
    type Error = anyhow::Error;

    fn try_from(status_code: char) -> anyhow::Result<Self> {
        match status_code {
            '-' => Ok(HeaderSliceStatus::Empty),
            '<' => Ok(HeaderSliceStatus::Waiting),
            '.' => Ok(HeaderSliceStatus::Downloaded),
            '=' => Ok(HeaderSliceStatus::VerifiedInternally),
            '#' => Ok(HeaderSliceStatus::Verified),
            'x' => Ok(HeaderSliceStatus::Invalid),
            'Y' => Ok(HeaderSliceStatus::Fork),
            '+' => Ok(HeaderSliceStatus::Saved),
            _ => Err(anyhow::format_err!(
                "unrecognized status code '{:?}'",
                status_code
            )),
        }
    }
}
