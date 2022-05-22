pub mod node;
pub mod types;

pub mod collections {
    use crate::models::{BlockHeader, BlockNumber, H256};
    use ethnum::U256;
    use hashbrown::HashSet;
    use hashlink::LruCache;

    #[derive(Debug, PartialEq, Eq, Clone, Default, Hash)]
    pub struct Link {
        height: BlockNumber,
        hash: H256,
        parent_hash: H256,
    }

    type Depth = usize;

    #[derive(Debug)]
    pub struct Graph {
        head: Link,
        chains: LruCache<H256, (U256, Depth, H256)>,
        reorged: bool,

        skip_list: LruCache<H256, HashSet<H256>>,
        raw: LruCache<H256, BlockHeader>,
        q: LruCache<Link, ()>,
        lru: LruCache<BlockNumber, HashSet<H256>>,
    }

    impl Graph {
        const CHAINS_CAP: usize = 1 << 8;
        const CACHE_CAP: usize = 3 << 17;

        pub fn new() -> Self {
            Self {
                head: Default::default(),
                chains: LruCache::new(Self::CHAINS_CAP),
                reorged: Default::default(),
                skip_list: LruCache::new(Self::CACHE_CAP),
                raw: LruCache::new(Self::CACHE_CAP),
                q: LruCache::new(Self::CACHE_CAP),
                lru: LruCache::new(Self::CACHE_CAP),
            }
        }

        pub fn contains(&mut self, number: &BlockNumber) -> bool {
            match self.lru.get(number) {
                Some(set) => set
                    .iter()
                    .map(|hash| self.raw.get(hash).unwrap().parent_hash)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .any(|parent_hash| self.raw.contains_key(&parent_hash)),
                None => false,
            }
        }

        pub fn len(&self) -> usize {
            self.raw.len()
        }

        pub fn is_empty(&self) -> bool {
            self.raw.is_empty()
        }

        pub fn insert(&mut self, header: BlockHeader) {
            let hash = header.hash();
            if self.raw.contains_key(&hash) {
                return;
            }

            let link = Link {
                height: header.number,
                hash,
                parent_hash: header.parent_hash,
            };
            self.skip_list
                .entry(header.parent_hash)
                .or_insert(HashSet::new())
                .insert(hash);
            self.lru
                .entry(header.number)
                .or_insert(HashSet::new())
                .insert(hash);
            self.raw.insert(hash, header);
            self.q.insert(link, ());
        }

        pub fn dfs(&mut self) -> Option<H256> {
            let mut roots = HashSet::new();

            for (node, _) in self.q.iter() {
                // Check if we don't have node's parent, and if it's the case - node is root.
                if !self.raw.contains_key(&node.parent_hash) {
                    roots.insert(node.hash);
                }
            }
            if roots.is_empty() {
                return None;
            }

            let mut stack = Vec::with_capacity(roots.len());
            for root in roots {
                stack.push(root);
            }

            let mut branches = HashSet::with_capacity(stack.len());

            // DFS and then backtrack.
            while let Some(hash) = stack.pop() {
                if let Some(next) = self.skip_list.get(&hash) {
                    for &child in next {
                        stack.push(child);
                    }
                } else {
                    branches.insert(hash);
                }
            }

            for branch in branches {
                let mut td = self
                    .chains
                    .get(&branch)
                    .map(|(td, _, _)| *td)
                    .unwrap_or_default();
                let mut parent = branch;
                let mut depth = 0;

                while let Some(header) = self.raw.get(&parent) {
                    // Break loop if we're branching on the same chain.
                    if self.chains.contains_key(&header.hash()) {
                        break;
                    }

                    td += header.difficulty;
                    parent = header.parent_hash;
                    depth += 1;
                }
                self.chains.insert(branch, (td, depth, parent));
            }

            if let Some((head_hash, (_, _, ancestor_hash))) =
                self.chains.iter().max_by_key(|(_, (td, _, _))| *td)
            {
                self.reorged = !(self.head.hash == *ancestor_hash);

                let header = self.raw.get(&head_hash).unwrap();

                self.head = Link {
                    height: header.number,
                    hash: *head_hash,
                    parent_hash: header.parent_hash,
                };
                Some(*head_hash)
            } else {
                None
            }
        }

        pub fn backtrack(&mut self, tail: &H256) -> Vec<BlockHeader> {
            let cap = self
                .chains
                .get(tail)
                .map(|(_, depth, _)| *depth)
                .expect("Tail is not in the graph");
            let mut headers = Vec::with_capacity(cap);

            let mut current = *tail;
            while let Some(header) = self.raw.remove(&current) {
                current = header.parent_hash;
                headers.push(header);
            }
            headers.reverse();
            headers
        }
    }

    impl Extend<BlockHeader> for Graph {
        fn extend<T>(&mut self, iter: T)
        where
            T: IntoIterator<Item = BlockHeader>,
        {
            for header in iter {
                self.insert(header);
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn test_max_td() {
            let mut graph = Graph::new();
            let mut parent_hash = H256::default();

            // We're starting from the forked chain.
            for number in (0..10u64).map(BlockNumber) {
                let header = BlockHeader {
                    parent_hash,
                    number,
                    difficulty: U256::from(1 * 10u64),
                    ..Default::default()
                };
                parent_hash = header.hash();
                graph.insert(header);
            }
            assert_eq!(graph.dfs().unwrap(), parent_hash);
            assert!(!graph.reorged);
            for number in (10..20u64).map(BlockNumber) {
                let header = BlockHeader {
                    parent_hash,
                    number,
                    difficulty: U256::from(1 * 10u64),
                    ..Default::default()
                };
                parent_hash = header.hash();
                graph.insert(header);
            }
            assert_eq!(graph.dfs().unwrap(), parent_hash);
            assert!(!graph.reorged);

            // Insert chain with higher difficulty.
            let mut canonical_parent_hash = H256::default();
            for number in (0..10u64).map(BlockNumber) {
                let header = BlockHeader {
                    parent_hash: canonical_parent_hash,
                    number,
                    difficulty: U256::from(1000 * 10u64),
                    ..Default::default()
                };
                canonical_parent_hash = header.hash();
                graph.insert(header);
            }

            // Insert more blocks from the forked chain.
            for number in (30..40u64).map(BlockNumber) {
                let header = BlockHeader {
                    parent_hash,
                    number,
                    difficulty: U256::from(1 * 10u64),
                    ..Default::default()
                };
                parent_hash = header.hash();
                graph.insert(header);
            }
            assert_eq!(graph.dfs().unwrap(), canonical_parent_hash);
            assert!(graph.reorged);

            let mut hash = H256::default();
            for number in (0..10).map(BlockNumber) {
                let header = BlockHeader {
                    parent_hash: hash,
                    number,
                    difficulty: U256::from(10000000000 * 10u64),
                    ..Default::default()
                };
                hash = header.hash();
                graph.insert(header);
            }
            assert_eq!(graph.dfs().unwrap(), hash);
            assert!(graph.reorged);
        }
    }
}
