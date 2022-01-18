use ethereum_types::H256;

fn assert_subset(sub: u16, sup: u16) {
    assert_eq!(sub & sup, sub);
}

#[derive(Clone, PartialEq)]
pub(crate) struct Node {
    state_mask: u16,
    tree_mask: u16,
    hash_mask: u16,
    hashes: Vec<H256>,
    root_hash: Option<H256>,
}

impl Node {
    fn new(
        state_mask: u16,
        tree_mask: u16,
        hash_mask: u16,
        hashes: Vec<H256>,
        root_hash: Option<H256>,
    ) -> Self {
        assert_subset(tree_mask, state_mask);
        assert_subset(hash_mask, state_mask);
        assert_eq!(hash_mask.count_ones() as usize, hashes.len());
        Self {
            state_mask,
            tree_mask,
            hash_mask,
            hashes,
            root_hash,
        }
    }

    fn assign(&mut self, other: &Node) {
        self.state_mask = other.state_mask;
        self.tree_mask = other.tree_mask;
        self.hash_mask = other.hash_mask;
        self.hashes = other.hashes.clone();
        self.root_hash = other.root_hash;
    }

    pub(crate) fn state_mask(&self) -> u16 {
        self.state_mask
    }

    pub(crate) fn tree_mask(&self) -> u16 {
        self.tree_mask
    }

    pub(crate) fn hash_mask(&self) -> u16 {
        self.hash_mask
    }

    pub(crate) fn hashes(&self) -> Vec<H256> {
        self.hashes.clone()
    }

    pub(crate) fn root_hash(&self) -> Option<H256> {
        self.root_hash
    }

    fn set_root_hash(&mut self, root_hash: Option<H256>) {
        self.root_hash = root_hash;
    }
}

fn marshal_node(node: &Node) -> Vec<u8> {
    todo!();
}

fn unmarshal_node(v: &[u8]) -> Option<Node> {
    todo!();
}
