use crate::models::{BlockHeader as Header, BlockNumber};
use parking_lot::RwLock;
use std::{
    collections::{HashMap, LinkedList},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time,
};
use strum::IntoEnumIterator;
use tokio::sync::watch;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum HeaderSliceStatus {
    // initialized, needs to be obtained
    Empty,
    // fetch request sent to sentry
    Waiting,
    // received from sentry
    Downloaded,
    // block hashes are matching the expected ones
    Verified,
    // saved in the database
    Saved,
}

pub struct HeaderSlice {
    pub start_block_num: BlockNumber,
    pub status: HeaderSliceStatus,
    pub headers: Option<Vec<Header>>,
    pub request_time: Option<time::Instant>,
    pub request_attempt: u16,
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
    slices: RwLock<LinkedList<RwLock<HeaderSlice>>>,
    max_slices: usize,
    max_block_num: AtomicU64,
    final_block_num: BlockNumber,
    state_watches: HashMap<HeaderSliceStatus, HeaderSliceStatusWatch>,
}

pub const HEADER_SLICE_SIZE: usize = 192;

const ATOMIC_ORDERING: Ordering = Ordering::SeqCst;

impl HeaderSlices {
    pub fn new(mem_limit: usize, final_block_num: BlockNumber) -> Self {
        let max_slices = mem_limit / std::mem::size_of::<Header>() / HEADER_SLICE_SIZE;

        assert_eq!(
            (final_block_num.0 as usize) % HEADER_SLICE_SIZE,
            0,
            "final_block_num must be at the slice boundary"
        );
        let max_slices =
            std::cmp::min(max_slices, (final_block_num.0 as usize) / HEADER_SLICE_SIZE);

        let mut slices = LinkedList::new();
        for i in 0..max_slices {
            let slice = HeaderSlice {
                start_block_num: BlockNumber((i * HEADER_SLICE_SIZE) as u64),
                status: HeaderSliceStatus::Empty,
                headers: None,
                request_time: None,
                request_attempt: 0,
            };
            slices.push_back(RwLock::new(slice));
        }

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

        Self {
            slices: RwLock::new(slices),
            max_slices,
            max_block_num: AtomicU64::new((max_slices * HEADER_SLICE_SIZE) as u64),
            final_block_num,
            state_watches,
        }
    }

    pub fn clone_statuses(&self) -> Vec<HeaderSliceStatus> {
        self.slices
            .read()
            .iter()
            .map(|slice| slice.read().status)
            .collect::<Vec<HeaderSliceStatus>>()
    }

    pub fn for_each<F>(&self, mut f: F) -> anyhow::Result<()>
    where
        F: FnMut(&RwLock<HeaderSlice>) -> Option<anyhow::Result<()>>,
    {
        for slice_lock in self.slices.read().iter() {
            let result_opt = f(slice_lock);
            if let Some(result) = result_opt {
                return result;
            }
        }
        Ok(())
    }

    pub fn find<F>(&self, start_block_num: BlockNumber, f: F)
    where
        F: FnOnce(Option<&RwLock<HeaderSlice>>),
    {
        let slices = self.slices.read();
        let slice_lock_opt = slices
            .iter()
            .find(|slice| slice.read().start_block_num == start_block_num);
        f(slice_lock_opt);
    }

    pub fn remove(&self, status: HeaderSliceStatus) {
        let mut slices = self.slices.write();
        let mut cursor = slices.cursor_front_mut();
        let mut count: usize = 0;

        while cursor.current().is_some() {
            let current_status = cursor.current().unwrap().read().status;
            if current_status == status {
                cursor.remove_current();
                count += 1;
            } else {
                cursor.move_next();
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
                status: HeaderSliceStatus::Empty,
                headers: None,
                request_time: None,
                request_attempt: 0,
            };
            slices.push_back(RwLock::new(slice));
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
}
