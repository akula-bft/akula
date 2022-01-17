use super::header::BlockHeader;
use crate::{models::BlockNumber, sentry::sentry_client::PeerId};
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
    // request refetching to try linking an alternative slice
    Refetch,
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
    pub fork_status: HeaderSliceStatus,
    pub fork_headers: Option<Vec<BlockHeader>>,
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
        let max_slices = mem_limit / std::mem::size_of::<BlockHeader>() / HEADER_SLICE_SIZE;

        assert_eq!(
            (start_block_num.0 as usize) % HEADER_SLICE_SIZE,
            0,
            "start_block_num must be at the slice boundary"
        );
        assert_eq!(
            (final_block_num.0 as usize) % HEADER_SLICE_SIZE,
            0,
            "final_block_num must be at the slice boundary"
        );

        let total_block_num = final_block_num.0 as usize - start_block_num.0 as usize;
        let max_slices = std::cmp::min(max_slices, total_block_num / HEADER_SLICE_SIZE);

        let mut slices = VecDeque::new();
        for i in 0..max_slices {
            let slice = HeaderSlice {
                start_block_num: BlockNumber(start_block_num.0 + (i * HEADER_SLICE_SIZE) as u64),
                ..Default::default()
            };
            slices.push_back(Arc::new(RwLock::new(slice)));
        }

        let max_block_num = start_block_num.0 + (max_slices * HEADER_SLICE_SIZE) as u64;

        let state_watches = Self::make_state_watches(max_slices);

        Self {
            slices: RwLock::new(slices),
            max_slices,
            max_block_num: AtomicU64::new(max_block_num),
            final_block_num,
            state_watches,
        }
    }

    #[cfg(test)]
    pub fn from_slices_vec(slices: Vec<HeaderSlice>) -> Self {
        let max_slices = slices.len();
        assert!(max_slices > 0, "slices must not be empty");

        let start_block_num = slices[0].start_block_num;
        let max_block_num = start_block_num.0 + (max_slices * HEADER_SLICE_SIZE) as u64;

        let state_watches = Self::make_state_watches_from_slices(&slices);

        let slice_locks =
            VecDeque::from_iter(slices.into_iter().map(|slice| Arc::new(RwLock::new(slice))));

        Self {
            slices: RwLock::new(slice_locks),
            max_slices,
            max_block_num: AtomicU64::new(max_block_num),
            final_block_num: BlockNumber(max_block_num),
            state_watches,
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

    fn make_state_watches(max_slices: usize) -> HashMap<HeaderSliceStatus, HeaderSliceStatusWatch> {
        let mut state_watches = HashMap::<HeaderSliceStatus, HeaderSliceStatusWatch>::new();
        for id in HeaderSliceStatus::iter() {
            let initial_count = if id == HeaderSliceStatus::Empty {
                max_slices
            } else {
                0
            };

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
        let slice_opt = self.find_by_start_block_num(start_block_num);
        slice_opt.and_then(|slice_lock| {
            if slice_lock.read().contains_block_num(block_num) {
                Some(slice_lock)
            } else {
                None
            }
        })
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

    pub fn has_one_of_statuses(&self, statuses: &[HeaderSliceStatus]) -> bool {
        statuses
            .iter()
            .any(|status| self.count_slices_in_status(*status) > 0)
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

impl HeaderSlice {
    pub fn len(&self) -> usize {
        self.headers.as_ref().map_or(0, |headers| headers.len())
    }

    pub fn block_num_range(&self) -> std::ops::Range<BlockNumber> {
        let end = BlockNumber(self.start_block_num.0 + self.len() as u64);
        self.start_block_num..end
    }

    pub fn contains_block_num(&self, num: BlockNumber) -> bool {
        self.block_num_range().contains(&num)
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
            HeaderSliceStatus::Refetch => 'R',
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
            'R' => Ok(HeaderSliceStatus::Refetch),
            '+' => Ok(HeaderSliceStatus::Saved),
            _ => Err(anyhow::format_err!(
                "unrecognized status code '{:?}'",
                status_code
            )),
        }
    }
}
