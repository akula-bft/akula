use crate::trie::util::has_prefix;

#[derive(Clone)]
pub(crate) struct PrefixSet {
    keys: Vec<Vec<u8>>,
    sorted: bool,
    index: usize,
}

impl PrefixSet {
    pub(crate) fn new() -> Self {
        Self {
            keys: vec![],
            sorted: false,
            index: 0,
        }
    }

    fn assign(&mut self, other: &PrefixSet) {
        self.keys = other.keys.clone();
        self.sorted = other.sorted;
        self.index = other.index;
    }

    pub(crate) fn contains(&mut self, prefix: &[u8]) -> bool {
        if self.keys.is_empty() {
            return false;
        }

        if !self.sorted {
            self.keys.sort();
            self.keys.dedup();
            self.sorted = true;
        }

        assert!(self.index < self.keys.len());
        while self.index > 0 && self.keys[self.index].as_slice() > prefix {
            self.index -= 1;
        }

        loop {
            if has_prefix(&self.keys[self.index], prefix) {
                break true;
            }
            if self.keys[self.index].as_slice() > prefix {
                break false;
            }
            if self.index == self.keys.len() - 1 {
                break false;
            }
            self.index += 1;
        }
    }

    pub(crate) fn insert(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
        self.sorted = false;
    }
}
