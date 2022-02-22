use crate::{
    crypto::keccak256,
    models::*,
    trie::{
        node::Node,
        util::{assert_subset, prefix_length},
    },
};
use bytes::{BufMut, Bytes, BytesMut};
use ethereum_types::H256;
use fastrlp::{Encodable, RlpEncodable, EMPTY_STRING_CODE};
use std::{boxed::Box, cmp};

const RLP_EMPTY_STRING_CODE: u8 = 0x80;

fn encode_path(nibbles: &[u8], terminating: bool) -> Vec<u8> {
    let mut res = vec![0u8; nibbles.len() / 2 + 1];
    let odd = nibbles.len() % 2 != 0;
    let mut i = 0usize;

    res[0] = if terminating { 0x20 } else { 0x00 };
    res[0] += if odd { 0x10 } else { 0x00 };

    if odd {
        res[0] |= nibbles[0];
        i = 1;
    }

    for byte in res.iter_mut().skip(1) {
        *byte = (nibbles[i] << 4) + nibbles[i + 1];
        i += 2;
    }

    res
}

fn wrap_hash(hash: &H256) -> Vec<u8> {
    [
        [RLP_EMPTY_STRING_CODE + KECCAK_LENGTH as u8].as_slice(),
        hash.0.as_slice(),
    ]
    .concat()
}

fn node_ref(rlp: &[u8]) -> Vec<u8> {
    if rlp.len() < KECCAK_LENGTH {
        rlp.to_vec()
    } else {
        wrap_hash(&keccak256(rlp))
    }
}

type NodeCollector<'nc> = Box<dyn FnMut(&[u8], &Node) + Send + Sync + 'nc>;

#[derive(Clone)]
enum HashBuilderValue {
    Bytes(Vec<u8>),
    Hash(H256),
}

fn leaf_node_rlp(path: &[u8], value: &[u8]) -> BytesMut {
    let encoded_path = &encode_path(path, true);

    #[derive(RlpEncodable)]
    struct S<'a> {
        encoded_path: &'a [u8],
        value: &'a [u8],
    }

    let mut out = BytesMut::new();
    S {
        encoded_path,
        value,
    }
    .encode(&mut out);
    out
}

fn extension_node_rlp(path: &[u8], child_ref: &[u8]) -> BytesMut {
    let encoded_path = Bytes::from(encode_path(path, false));

    let mut out = BytesMut::new();
    let h = fastrlp::Header {
        list: true,
        payload_length: fastrlp::Encodable::length(&encoded_path) + child_ref.len(),
    };
    h.encode(&mut out);
    encoded_path.encode(&mut out);
    out.extend_from_slice(child_ref);
    out
}

pub(crate) struct HashBuilder<'nc> {
    pub(crate) node_collector: Option<NodeCollector<'nc>>,
    key: Vec<u8>,
    value: HashBuilderValue,
    is_in_db_trie: bool,
    groups: Vec<u16>,
    tree_masks: Vec<u16>,
    hash_masks: Vec<u16>,
    stack: Vec<Vec<u8>>,
}

impl<'nc> HashBuilder<'nc> {
    pub(crate) fn new(node_collector: Option<NodeCollector<'nc>>) -> Self {
        Self {
            node_collector,
            key: vec![],
            value: HashBuilderValue::Bytes(vec![]),
            is_in_db_trie: false,
            groups: vec![],
            tree_masks: vec![],
            hash_masks: vec![],
            stack: vec![],
        }
    }

    fn collects_nodes(&self) -> bool {
        self.node_collector.is_some()
    }

    pub(crate) fn add_leaf(&mut self, key: Vec<u8>, value: &[u8]) {
        assert!(key > self.key);
        if !self.key.is_empty() {
            self.gen_struct_step(key.as_slice());
        }
        self.key = key;
        self.value = HashBuilderValue::Bytes(value.to_vec());
    }

    pub(crate) fn add_branch_node(&mut self, key: Vec<u8>, value: &H256, is_in_db_trie: bool) {
        assert!(key > self.key || (self.key.is_empty() && key.is_empty()));
        if !self.key.is_empty() {
            self.gen_struct_step(key.as_slice());
        } else if key.is_empty() {
            self.stack.push(wrap_hash(value));
        }
        self.key = key;
        self.value = HashBuilderValue::Hash(*value);
        self.value = HashBuilderValue::Hash(*value);
        self.is_in_db_trie = is_in_db_trie;
    }

    pub(crate) fn compute_root_hash(&mut self) -> H256 {
        self.finalize();
        self.get_root_hash()
    }

    fn get_root_hash(&self) -> H256 {
        if let Some(node_ref) = self.stack.last() {
            if node_ref.len() == KECCAK_LENGTH + 1 {
                H256::from_slice(&node_ref[1..])
            } else {
                keccak256(node_ref)
            }
        } else {
            EMPTY_ROOT
        }
    }

    fn finalize(&mut self) {
        if !self.key.is_empty() {
            self.gen_struct_step(&[]);
            self.key.clear();
            self.value = HashBuilderValue::Bytes(vec![]);
        }
    }

    fn gen_struct_step(&mut self, succeeding: &[u8]) {
        let mut build_extensions = false;
        let mut current = self.key.clone();

        loop {
            let preceding_exists = !self.groups.is_empty();
            let preceding_len: usize = self.groups.len().saturating_sub(1);

            let common_prefix_len = prefix_length(succeeding, current.as_slice());
            let len = cmp::max(preceding_len, common_prefix_len);
            assert!(len < current.len());

            let extra_digit = current[len];
            if self.groups.len() <= len {
                self.groups.resize(len + 1, 0u16);
            }
            self.groups[len] |= 1u16 << extra_digit;

            if self.tree_masks.len() < current.len() {
                self.tree_masks.resize(current.len(), 0u16);
                self.hash_masks.resize(current.len(), 0u16);
            }

            let mut len_from = len;
            if !succeeding.is_empty() || preceding_exists {
                len_from += 1;
            }

            let short_node_key = current[len_from..].to_vec();
            if !build_extensions {
                let value = self.value.clone();
                match &value {
                    HashBuilderValue::Bytes(leaf_value) => {
                        let node_ref =
                            node_ref(leaf_node_rlp(short_node_key.as_slice(), leaf_value).as_ref());
                        self.stack.push(node_ref);
                    }
                    HashBuilderValue::Hash(hash) => {
                        self.stack.push(wrap_hash(hash));
                        if self.collects_nodes() {
                            if self.is_in_db_trie {
                                self.tree_masks[current.len() - 1] |=
                                    1u16 << current.last().unwrap();
                            }
                            self.hash_masks[current.len() - 1] |= 1u16 << current.last().unwrap();
                        }
                        build_extensions = true;
                    }
                }
            }

            if build_extensions && !short_node_key.is_empty() {
                if self.collects_nodes() && len_from > 0 {
                    let flag = 1u16 << current[len_from - 1];

                    self.hash_masks[len_from - 1] &= !flag;

                    if self.tree_masks[current.len() - 1] != 0 {
                        self.tree_masks[len_from - 1] |= flag;
                    }
                }

                let stack_last = self.stack.pop().unwrap();
                let new_stack_last = node_ref(
                    extension_node_rlp(short_node_key.as_slice(), stack_last.as_slice()).as_ref(),
                );
                self.stack.push(new_stack_last);

                self.hash_masks.resize(len_from, 0u16);
                self.tree_masks.resize(len_from, 0u16);
            }

            if preceding_len <= common_prefix_len && !succeeding.is_empty() {
                return;
            }

            if !succeeding.is_empty() || preceding_exists {
                let child_hashes = self.branch_ref(self.groups[len], self.hash_masks[len]);

                if self.collects_nodes() {
                    if len > 0 {
                        self.hash_masks[len - 1] |= 1u16 << current[len - 1];
                    }

                    let store_in_db_trie = self.tree_masks[len] != 0 || self.hash_masks[len] != 0;
                    if store_in_db_trie {
                        if len > 0 {
                            self.tree_masks[len - 1] |= 1u16 << current[len - 1];
                        }

                        let hashes = child_hashes
                            .iter()
                            .map(|hash| H256::from_slice(&hash[1..]))
                            .collect();

                        let mut n = Node::new(
                            self.groups[len],
                            self.tree_masks[len],
                            self.hash_masks[len],
                            hashes,
                            None,
                        );

                        if len == 0 {
                            n.root_hash = Some(self.get_root_hash());
                        }

                        self.node_collector.as_mut().unwrap()(&current[0..len], &n);
                    }
                }
            }

            self.groups.resize(len, 0u16);
            self.tree_masks.resize(len, 0u16);
            self.hash_masks.resize(len, 0u16);

            if preceding_len == 0 {
                return;
            }

            current.truncate(preceding_len);
            while self.groups.last() == Some(&0) {
                self.groups.pop();
            }

            build_extensions = true;
        }
    }

    fn branch_ref(&mut self, state_mask: u16, hash_mask: u16) -> Vec<Vec<u8>> {
        assert_subset(hash_mask, state_mask);
        let mut child_hashes = Vec::<Vec<u8>>::with_capacity(hash_mask.count_ones() as usize);
        let first_child_idx = self.stack.len() - state_mask.count_ones() as usize;

        let mut rlp_buffer = BytesMut::new();
        {
            // Length for the nil value added below
            let mut h = fastrlp::Header {
                list: true,
                payload_length: 1,
            };
            let mut i = first_child_idx;
            for digit in 0..16 {
                if state_mask & (1u16 << digit) != 0 {
                    h.payload_length += self.stack[i].len();
                    i += 1;
                } else {
                    h.payload_length += 1;
                }
            }
            h.encode(&mut rlp_buffer);
        }
        {
            let mut i = first_child_idx;
            for digit in 0..16 {
                if state_mask & (1u16 << digit) != 0 {
                    if hash_mask & (1u16 << digit) != 0 {
                        child_hashes.push(self.stack[i].clone());
                    }
                    rlp_buffer.extend_from_slice(&self.stack[i]);
                    i += 1;
                } else {
                    rlp_buffer.put_u8(EMPTY_STRING_CODE);
                }
            }
        }

        // branch nodes with values are not supported
        rlp_buffer.put_u8(EMPTY_STRING_CODE);

        self.stack.resize(first_child_idx, vec![]);
        self.stack.push(node_ref(&rlp_buffer));

        child_hashes
    }
}

pub(crate) fn pack_nibbles(nibbles: &[u8]) -> Vec<u8> {
    let n = (nibbles.len() + 1) / 2;
    let mut out = vec![0u8; n];
    if n == 0 {
        return out;
    }

    let mut i = 0;
    let mut j = 0;
    while j < nibbles.len() {
        out[i] = nibbles[j] << 4;
        j += 1;
        if j < nibbles.len() {
            out[i] += nibbles[j];
            j += 1;
            i += 1;
        }
    }

    out
}

pub(crate) fn unpack_nibbles(packed: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; packed.len() * 2];
    let mut i = 0;
    for b in packed {
        out[i] = b >> 4;
        out[i + 1] = b & 0x0F;
        i += 2;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::trie_root;
    use fastrlp::RlpEncodable;
    use hex_literal::hex;

    #[test]
    fn test_hash_builder_empty_trie() {
        let mut hb = HashBuilder::new(None);
        assert_eq!(hb.compute_root_hash(), EMPTY_ROOT);
    }

    #[test]
    fn test_hash_builder_1() {
        let key1 = H256::from_low_u64_be(1);
        let key2 = H256::from_low_u64_be(2);

        let val1 = vec![1u8];
        let val2 = vec![2u8];

        let mut hb = HashBuilder::new(None);
        hb.add_leaf(unpack_nibbles(key1.as_bytes()), val1.as_slice());
        hb.add_leaf(unpack_nibbles(key2.as_bytes()), val2.as_slice());

        let hash = trie_root(vec![(key1, val1), (key2, val2)]);
        let root_hash = hb.compute_root_hash();
        assert_eq!(hash, root_hash);
    }

    #[test]
    fn test_hash_builder_2() {
        let key0 = hex!("646f").to_vec();
        let val0 = hex!("76657262").to_vec();

        let mut hb0 = HashBuilder::new(None);
        hb0.add_leaf(unpack_nibbles(key0.as_slice()), val0.as_slice());

        let hash0 = trie_root(vec![(key0.clone(), val0.clone())]);
        assert_eq!(hb0.compute_root_hash(), hash0);

        let key1 = hex!("676f6f64").to_vec();
        let val1 = hex!("7075707079").to_vec();

        let mut hb1 = HashBuilder::new(None);
        hb1.add_leaf(unpack_nibbles(key0.as_slice()), val0.as_slice());
        hb1.add_leaf(unpack_nibbles(key1.as_slice()), val1.as_slice());

        let hash1 = trie_root(vec![
            (key0.clone(), val0.clone()),
            (key1.clone(), val1.clone()),
        ]);
        let hash1b = hash1;
        assert_eq!(hb1.compute_root_hash(), hash1);

        #[derive(RlpEncodable)]
        struct S<'a> {
            a: &'a [u8],
            b: &'a [u8],
        }

        let path0 = encode_path(unpack_nibbles(&key0[1..]).as_slice(), true);
        let entry0 = S {
            a: &path0,
            b: &val0,
        };

        let path1 = encode_path(unpack_nibbles(&key1[1..]).as_slice(), true);
        let entry1 = S {
            a: &path1,
            b: &val1,
        };

        let mut enc: [&dyn Encodable; 17] = [b""; 17];
        enc[4] = &entry0;
        enc[7] = &entry1;
        let mut branch_node_rlp = BytesMut::new();
        fastrlp::encode_list::<dyn Encodable, _>(&enc, &mut branch_node_rlp);

        let branch_node_hash = keccak256(branch_node_rlp);

        let mut hb2 = HashBuilder::new(None);
        hb2.add_branch_node(vec![0x6], &branch_node_hash, false);

        assert_eq!(hb2.compute_root_hash(), hash1b);
    }

    #[test]
    fn test_hash_builder_known_root_hash() {
        let root_hash = H256::from(hex!(
            "9fa752911d55c3a1246133fe280785afbdba41f357e9cae1131d5f5b0a078b9c"
        ));
        let mut hb = HashBuilder::new(None);
        hb.add_branch_node(vec![], &root_hash, false);
        assert_eq!(hb.compute_root_hash(), root_hash);
    }

    #[test]
    fn test_hash_builder_pack_nibbles() {
        assert_eq!(pack_nibbles(&[]), Vec::<u8>::new());
        assert_eq!(pack_nibbles(&[0xa]), vec![0xa0]);
        assert_eq!(pack_nibbles(&[0xa, 0xb]), vec![0xab]);
        assert_eq!(pack_nibbles(&[0xa, 0xb, 0x2]), vec![0xab, 0x20]);
        assert_eq!(pack_nibbles(&[0xa, 0xb, 0x2, 0x0]), vec![0xab, 0x20]);
        assert_eq!(pack_nibbles(&[0xa, 0xb, 0x2, 0x7]), vec![0xab, 0x27]);
    }
}
