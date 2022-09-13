use crate::trie::util::is_subset;
use crate::{models::KECCAK_LENGTH, trie::util::assert_subset};
use anyhow::{bail, Result};
use ethereum_types::H256;

#[derive(Clone, Debug, PartialEq, Eq)]
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

    pub(crate) fn reset(&mut self) {
        self.hash_mask = 0;
        self.state_mask = 0;
        self.tree_mask = 0;
        self.hashes.clear();
        self.root_hash = None;
    }

    pub fn hash_for_nibble(&self, nibble: i8) -> H256 {
        let mask = (1u16 << nibble) - 1;
        let index = (self.hash_mask & mask).count_ones();
        self.hashes[index as usize]
    }

    pub(crate) fn encode_for_storage(&self) -> Vec<u8> {
        let number_of_hashes = self.hashes.len() + if self.root_hash.is_some() { 1 } else { 0 };
        let buf_size = number_of_hashes * KECCAK_LENGTH + 6;
        let mut buf = Vec::<u8>::with_capacity(buf_size);

        buf.extend_from_slice(self.state_mask.to_be_bytes().as_slice());
        buf.extend_from_slice(self.tree_mask.to_be_bytes().as_slice());
        buf.extend_from_slice(self.hash_mask.to_be_bytes().as_slice());

        if let Some(root_hash) = self.root_hash {
            buf.extend_from_slice(root_hash.as_bytes());
        }
        for hash in &self.hashes {
            buf.extend_from_slice(hash.as_bytes());
        }

        buf
    }

    pub(crate) fn decode_from_storage(raw: &[u8]) -> Result<Self> {
        if raw.len() % KECCAK_LENGTH != 6 {
            bail!("Invalid length of raw data");
        }

        let state_mask = u16::from_be_bytes(raw[0..2].try_into().unwrap());
        let tree_mask = u16::from_be_bytes(raw[2..4].try_into().unwrap());
        let hash_mask = u16::from_be_bytes(raw[4..6].try_into().unwrap());

        if !is_subset(tree_mask, state_mask) || !is_subset(hash_mask, state_mask) {
            bail!("Invalid masks");
        }

        let mut i = 6;

        let found_hashes = (raw.len() - 6) / KECCAK_LENGTH;
        let expected_hashes = hash_mask.count_ones() as usize;

        let root_hash = if found_hashes == expected_hashes + 1 {
            i += KECCAK_LENGTH;
            Some(H256::from_slice(&raw[6..6 + KECCAK_LENGTH]))
        } else if found_hashes == expected_hashes {
            None
        } else {
            bail!("Wrong number of hashes in marshalled data")
        };

        let mut hashes = Vec::<H256>::with_capacity(expected_hashes);
        for _ in 0..expected_hashes {
            hashes.push(H256::from_slice(&raw[i..i + KECCAK_LENGTH]));
            i += KECCAK_LENGTH;
        }

        Ok(Node::new(
            state_mask, tree_mask, hash_mask, hashes, root_hash,
        ))
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new(0, 0, 0, vec![], None)
    }
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

        let encoded = n.encode_for_storage();
        let decoded = Node::decode_from_storage(&encoded).unwrap();
        assert_eq!(decoded, n);
    }

    #[test]
    fn invalid_node_decoding() {
        // empty
        let testee = Node::decode_from_storage(&[]);
        assert!(testee.is_err());

        // only state mask
        let testee = Node::decode_from_storage(&hex!("f607"));
        assert!(testee.is_err());

        // two bits set in hash mask, but no hashes
        let testee = Node::decode_from_storage(&hex!("f60700054004"));
        assert!(testee.is_err());

        // to short data for two bits set in hash mask
        let testee = Node::decode_from_storage(&hex!(
            "f60700054004aaaabbbb0006767767776fffffeee444440000055676456000"
        ));
        assert!(testee.is_err());

        // too many hashes
        let testee = Node::decode_from_storage( &hex!(
            "f60700054004aaaabbbb0006767767776fffffeee44444000005567645600000000eeddddddd90d53cd810cc5d4243766cd4451e7b9d14b736a1148b26b3baac7617f617d321cc35c964dda53ba6c0b87798073a9628dbc9cd26b5cce88eb69655a9c609caf1cc35c964dda53ba6c0b87798073a9628dbc9cd26b5cce88eb69655a9c609caf1"
        ));
        assert!(testee.is_err());

        // tree and hash masks not subset of state mask
        let testee = Node::decode_from_storage(&hex!("000000054004"));
        assert!(testee.is_err());
    }
}
