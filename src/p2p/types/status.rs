use crate::models::{BlockNumber, H256};
use parking_lot::Mutex;

pub struct AtomicStatus(Mutex<Status>);

impl AtomicStatus {
    #[inline(always)]
    pub fn new(status: Status) -> Self {
        Self(Mutex::new(status))
    }
    #[inline(always)]
    pub fn load(&self) -> Status {
        *self.0.lock()
    }
    #[inline(always)]
    pub fn store(&self, status: Status) {
        *self.0.lock() = status;
    }
}

impl std::fmt::Debug for AtomicStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AtomicStatus {{ {:?} }}", self.load())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Status {
    pub height: BlockNumber,
    pub hash: H256,
    pub total_difficulty: H256,
}

impl PartialEq for Status {
    #[inline(always)]
    fn eq(&self, other: &Status) -> bool {
        self.height == other.height
            && self.hash == other.hash
            && self.total_difficulty == other.total_difficulty
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_atomic_status() {
        use super::*;
        let status = Status {
            height: 1.into(),
            hash: H256::default(),
            total_difficulty: H256::default(),
        };
        let atomic_status = AtomicStatus::new(status);
        assert_eq!(atomic_status.load(), status);
        atomic_status.store(status);
        assert_eq!(atomic_status.load(), status);
        let new_status = Status {
            height: 2.into(),
            hash: H256::default(),
            total_difficulty: H256::default(),
        };
        atomic_status.store(new_status);
        assert_eq!(atomic_status.load(), new_status);
    }
}
