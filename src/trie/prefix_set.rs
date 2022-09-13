use crate::trie::util::has_prefix;
use bytes::Bytes;
use std::cmp::min;

#[derive(Clone)]
pub struct PrefixSet {
    keys: Vec<(Bytes, bool)>,
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

    #[inline]
    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.keys.sort();
            self.keys.dedup();
            self.sorted = true;
        }
    }

    pub(crate) fn contains(&mut self, prefix: &[u8]) -> bool {
        if self.keys.is_empty() {
            return false;
        }

        self.ensure_sorted();

        while self.index > 0 && self.keys[self.index].0 > prefix {
            self.index -= 1;
        }

        loop {
            let current = &self.keys[self.index].0;

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
        self.insert2(key, false);
    }

    pub fn insert2(&mut self, key: &[u8], marker: bool) {
        self.keys.push((Bytes::copy_from_slice(key), marker));
        self.sorted = false;
    }

    pub(crate) fn clear(&mut self) {
        self.keys.clear();
        self.index = 0;
        self.sorted = false;
    }

    pub(crate) fn contains_and_next_sorted(
        &mut self,
        prefix: &[u8],
        invariant_prefix_len: usize,
    ) -> (bool, Vec<u8>) {
        let mut next_created = vec![];
        let invariant_prefix_len = min(prefix.len(), invariant_prefix_len);

        for (ref first, second) in self.keys.iter().skip(self.index) {
            if first[0..invariant_prefix_len] == *prefix {
                break;
            }

            if *second {
                next_created = first.to_vec();
                break;
            }
        }

        (self.contains(prefix), next_created)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn prefix_set() {
        let mut ps = PrefixSet::new();
        assert_eq!(ps.len(), 0);
        assert!(!ps.contains(b""));
        assert!(!ps.contains(b"a"));

        ps.insert(b"abc");
        ps.insert(b"fg");
        ps.insert(b"abc"); // duplicate
        ps.insert2(b"abd", true);
        ps.insert(b"abc"); // duplicate
        ps.insert(b"ab");

        assert_eq!(ps.len(), 6);

        assert!(ps.contains(b""));
        assert!(ps.contains(b"a"));
        assert!(!ps.contains(b"aac"));
        assert!(ps.contains(b"ab"));
        assert!(ps.contains(b"abc"));

        let (contains, next_created) = ps.contains_and_next_sorted(b"abc", 0);
        assert!(contains);
        assert_eq!(next_created, Bytes::from_static(b"abd"));

        assert!(!ps.contains(b"abcd"));
        assert!(!ps.contains(b"b"));
        assert!(ps.contains(b"f"));
        assert!(ps.contains(b"fg"));
        assert!(!ps.contains(b"fgk"));
        assert!(!ps.contains(b"fy"));
        assert!(!ps.contains(b"yyz"));

        ps.clear();
        assert_eq!(ps.len(), 0);
    }

    #[ignore] // TODO
    #[test]
    fn prefix_set_storage_prefix() {
        let prefix1 = hex!(
            "00000c28401f2ddfc4ffb8231a088e59b082343dcf32292deb61832480c3f4f50000000000000001"
        )
        .to_vec();
        let prefix2 = hex!(
            "00000c28401f2ddfc4ffb8231a088e59b082343dcf32292deb61832480c3f4f50000000000000002"
        )
        .to_vec();

        let mut ps = PrefixSet::new();

        let keys = vec![
            (b"ab".as_slice(), false),
            (b"abc".as_slice(), false),
            (b"abd".as_slice(), true),
            (b"abe".as_slice(), false),
            (b"abf".as_slice(), true),
            (b"fg".as_slice(), false),
        ];

        for (item, marker) in &keys {
            let mut prefixed = prefix1.clone();
            prefixed.extend(*item);
            ps.insert2(&prefixed, *marker);
        }

        for (item, marker) in &keys {
            let mut prefixed = prefix2.clone();
            prefixed.extend(*item);
            ps.insert2(&prefixed, *marker);
        }

        let (contains, next_created) = ps.contains_and_next_sorted(&prefix1, prefix1.len());
        assert!(contains);
        assert_eq!(prefix1, next_created);
    }
}
