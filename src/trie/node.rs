use crate::{models::KECCAK_LENGTH, trie::util::assert_subset};
use ethereum_types::H256;

#[derive(Clone, Debug, PartialEq)]
pub struct Node {
    pub state_mask: u16,
    pub tree_mask: u16,
    pub hash_mask: u16,
    pub hashes: Vec<H256>,
    pub root_hash: Option<H256>,
}

impl Node {
    pub fn new(
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

    pub fn hash_for_nibble(&self, nibble: i8) -> H256 {
        let mask = (1u16 << nibble) - 1;
        let index = (self.hash_mask & mask).count_ones();
        self.hashes[index as usize]
    }
}

pub(crate) fn marshal_node(n: &Node) -> Vec<u8> {
    let number_of_hashes = n.hashes.len() + if n.root_hash.is_some() { 1 } else { 0 };
    let buf_size = number_of_hashes * KECCAK_LENGTH + 6;
    let mut buf = Vec::<u8>::with_capacity(buf_size);

    buf.extend_from_slice(n.state_mask.to_be_bytes().as_slice());
    buf.extend_from_slice(n.tree_mask.to_be_bytes().as_slice());
    buf.extend_from_slice(n.hash_mask.to_be_bytes().as_slice());

    if let Some(root_hash) = n.root_hash {
        buf.extend_from_slice(root_hash.as_bytes());
    }

    for hash in &n.hashes {
        buf.extend_from_slice(hash.as_bytes());
    }

    buf
}

pub(crate) fn unmarshal_node(v: &[u8]) -> Option<Node> {
    if v.len() % KECCAK_LENGTH != 6 {
        return None;
    }

    let state_mask = u16::from_be_bytes(v[0..2].try_into().unwrap());
    let tree_mask = u16::from_be_bytes(v[2..4].try_into().unwrap());
    let hash_mask = u16::from_be_bytes(v[4..6].try_into().unwrap());
    let mut i = 6;

    let mut root_hash = None;
    if hash_mask.count_ones() as usize + 1 == v[6..].len() / KECCAK_LENGTH {
        root_hash = Some(H256::from_slice(&v[i..i + KECCAK_LENGTH]));
        i += KECCAK_LENGTH;
    }

    let num_hashes = v[i..].len() / KECCAK_LENGTH;
    let mut hashes = Vec::<H256>::with_capacity(num_hashes);
    for _ in 0..num_hashes {
        hashes.push(H256::from_slice(&v[i..i + KECCAK_LENGTH]));
        i += KECCAK_LENGTH;
    }

    Some(Node::new(
        state_mask, tree_mask, hash_mask, hashes, root_hash,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex_literal::hex;

    #[test]
    fn node_marshalling() {
        let n = Node::new(
            0xf607,
            0x0005,
            0x4004,
            vec![
                hex!("90d53cd810cc5d4243766cd4451e7b9d14b736a1148b26b3baac7617f617d321").into(),
                hex!("cc35c964dda53ba6c0b87798073a9628dbc9cd26b5cce88eb69655a9c609caf1").into(),
            ],
            Some(hex!("aaaabbbb0006767767776fffffeee44444000005567645600000000eeddddddd").into()),
        );

        // REQUIRE(std::bitset<16>(n.hash_mask()).count() == n.hashes().size());

        assert_eq!(unmarshal_node(&marshal_node(&n)).unwrap(), n);
    }
}
