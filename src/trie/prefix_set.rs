use crate::trie::util::has_prefix;
use bytes::Bytes;

#[derive(Clone)]
pub struct PrefixSet {
    keys: Vec<Bytes>,
    sorted: bool,
    index: usize,
}

impl Default for PrefixSet {
    fn default() -> Self {
        PrefixSet::new()
    }
}

impl PrefixSet {
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            sorted: true,
            index: 0,
        }
    }

    fn sort(&mut self) {
        self.keys.sort();
        self.keys.dedup();
        self.sorted = true;
    }

    pub(crate) fn contains(&mut self, prefix: &[u8]) -> bool {
        if self.keys.is_empty() {
            return false;
        }

        if !self.sorted {
            self.sort();
        }

        while self.index > 0 && self.keys[self.index] > prefix {
            self.index -= 1;
        }

        loop {
            let current = &self.keys[self.index];

            if has_prefix(current, prefix) {
                break true;
            }

            if current > prefix {
                break false;
            }

            if self.index >= self.keys.len() - 1 {
                break false;
            }

            self.index += 1;
        }
    }

    pub fn insert(&mut self, key: &[u8]) {
        self.keys.push(Bytes::copy_from_slice(key));
        self.sorted = false;
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_set() {
        let mut ps = PrefixSet::new();
        assert!(!ps.contains(b""));
        assert!(!ps.contains(b"a"));

        ps.insert(b"abc");
        ps.insert(b"fg");
        ps.insert(b"abc"); // duplicate
        ps.insert(b"ab");

        assert!(ps.contains(b""));
        assert!(ps.contains(b"a"));
        assert!(!ps.contains(b"aac"));
        assert!(ps.contains(b"ab"));
        assert!(ps.contains(b"abc"));
        assert!(!ps.contains(b"abcd"));
        assert!(!ps.contains(b"b"));
        assert!(ps.contains(b"f"));
        assert!(ps.contains(b"fg"));
        assert!(!ps.contains(b"fgk"));
        assert!(!ps.contains(b"fy"));
        assert!(!ps.contains(b"yyz"));
    }
}
