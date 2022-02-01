use crate::trie::util::has_prefix;
use std::collections::BTreeSet;

#[derive(Clone)]
pub(crate) struct PrefixSet(BTreeSet<Vec<u8>>);

impl PrefixSet {
    pub(crate) fn new() -> Self {
        Self { 0: BTreeSet::new() }
    }

    pub(crate) fn contains(&mut self, prefix: &[u8]) -> bool {
        self.0
            .range(prefix.to_vec()..)
            .next()
            .map(|s| has_prefix(s, prefix))
            .unwrap_or(false)
    }

    pub(crate) fn insert(&mut self, key: &[u8]) {
        self.0.insert(key.to_vec());
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
