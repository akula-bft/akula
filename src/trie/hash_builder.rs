use crate::{
    crypto::keccak256,
    models::{EMPTY_ROOT, KECCAK_LENGTH},
    trie::{
        node::Node,
        util::{assert_subset, prefix_length},
    },
};
use ethereum_types::H256;
use rlp::RlpStream;
use std::{boxed::Box, cmp};
use crate::crypto::root_hash;

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
        assert_eq!(nibbles.len() % 2, 0);
    }

    for j in 0..res.len() {
        res[j] = nibbles[i] << 4 + nibbles[i + 1];
        i += 2;
    }

    res
}

fn wrap_hash(hash: &H256) -> Vec<u8> {
    let mut wrapped = vec![0u8; KECCAK_LENGTH + 1];
    wrapped[0] = RLP_EMPTY_STRING_CODE + KECCAK_LENGTH as u8;
    for i in 0..32 {
        wrapped[i + 1] = hash.0[i];
    }
    wrapped
}

fn node_ref(rlp: &[u8]) -> Vec<u8> {
    if rlp.len() < KECCAK_LENGTH {
        return rlp.to_vec();
    }
    let hash = H256::from_slice(rlp);
    wrap_hash(&hash)
}

type NodeCollector = Box<dyn Fn(&[u8], &Node) -> ()>;

#[derive(Clone)]
enum HashBuilderValue {
    Bytes(Vec<u8>),
    Hash(H256),
}

pub(crate) struct HashBuilder {
    node_collector: Option<NodeCollector>,
    key: Vec<u8>,
    value: HashBuilderValue,
    is_in_db_trie: bool,
    groups: Vec<u16>,
    tree_masks: Vec<u16>,
    hash_masks: Vec<u16>,
    stack: Vec<Vec<u8>>,
    rlp_buffer: Vec<u8>,
}

impl HashBuilder {
    fn new() -> Self {
        Self {
            node_collector: None,
            key: vec![],
            value: HashBuilderValue::Bytes(vec![]),
            is_in_db_trie: false,
            groups: vec![],
            tree_masks: vec![],
            hash_masks: vec![],
            stack: vec![],
            rlp_buffer: vec![],
        }
    }

    fn add_leaf(&mut self, key: Vec<u8>, value: &[u8]) {
        assert!(key > self.key);
        if !self.key.is_empty() {
            let self_key = self.key.clone();
            self.gen_struct_step(self_key.as_slice(), key.as_slice());
        }
        self.key = key;
        self.value = HashBuilderValue::Bytes(value.to_vec());
    }

    fn add_branch_node(&mut self, key: Vec<u8>, value: &H256, is_in_db_trie: bool) {
        assert!(key > self.key || (self.key.is_empty() && key.is_empty()));
        if !self.key.is_empty() {
            let self_key = self.key.clone();
            self.gen_struct_step(self_key.as_slice(), key.as_slice());
        } else if key.is_empty() {
            self.stack.push(wrap_hash(value));
        }
        self.key = key;
        self.value = HashBuilderValue::Hash(*value);
        self.is_in_db_trie = is_in_db_trie;
    }

    fn root_hash(&mut self) -> H256 {
        self.private_root_hash(true)
    }

    fn private_root_hash(&mut self, auto_finalize: bool) -> H256 {
        if auto_finalize {
            self.finalize();
        }

        if self.stack.is_empty() {
            return EMPTY_ROOT;
        }

        let node_ref = self.stack.last().unwrap();
        let mut res = H256::zero();
        if node_ref.len() == KECCAK_LENGTH + 1 {
            for i in 0..32 {
                res = H256::from_slice(&node_ref[1..]);
            }
        } else {
            res = keccak256(node_ref);
        }

        res
    }

    fn finalize(&mut self) {
        if !self.key.is_empty() {
            let self_key = self.key.clone();
            self.gen_struct_step(self_key.as_slice(), &[]);
            self.key.clear();
            self.value = HashBuilderValue::Bytes(vec![]);
        }
    }

    fn gen_struct_step(&mut self, current: &[u8], succeeding: &[u8]) {
        let mut build_extensions = false;
        let mut current = current.to_vec();

        loop {
            let preceding_exists = !self.groups.is_empty();

            let preceding_len = if self.groups.is_empty() {
                0
            } else {
                self.groups.len() - 1
            };
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

            let short_node_key = current[0..len_from].to_vec();
            if !build_extensions {
                let value = self.value.clone();
                match value {
                    HashBuilderValue::Bytes(ref leaf_value) => {
                        let x = node_ref(self.leaf_node_rlp(short_node_key.as_slice(), leaf_value)
                                .as_slice());
                        self.stack.push(node_ref(x.as_slice()));
                    },
                    HashBuilderValue::Hash(ref hash) => {
                        self.stack.push(wrap_hash(hash));
                        if self.node_collector.is_some() {
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
                if self.node_collector.is_some() && len_from > 0 {
                    let flag = 1u16 << current[len_from - 1];

                    self.hash_masks[len_from - 1] &= !flag;

                    if self.tree_masks[current.len() - 1] != 0 {
                        self.tree_masks[len_from - 1] |= flag;
                    }
                }

                let stack_last = self.stack.pop().unwrap();
                let new_stack_last = node_ref(
                    self.extension_node_rlp(short_node_key.as_slice(), stack_last.as_slice())
                        .as_slice(),
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

                let have_node_collector = self.node_collector.is_some();
                if have_node_collector {
                    if len > 0 {
                        self.hash_masks[len - 1] |= 1u16 << current[len - 1];
                    }

                    let store_in_db_trie = self.tree_masks[len] != 0 || self.hash_masks[len] != 0;
                    if store_in_db_trie {
                        if len > 0 {
                            self.tree_masks[len - 1] |= 1u16 << current[len - 1];
                        }
                        let mut hashes = Vec::<H256>::with_capacity(child_hashes.len());
                        for i in 0..child_hashes.len() {
                            assert_eq!(child_hashes[i].len(), KECCAK_LENGTH + 1);
                            hashes.push(H256::from_slice(&child_hashes[i][1..]))
                        }
                        let mut n = Node::new(
                            self.groups[len],
                            self.tree_masks[len],
                            self.hash_masks[len],
                            hashes,
                            None,
                        );
                        if len == 0 {
                            n.set_root_hash(Some(self.private_root_hash(false)));
                        }

                        self.node_collector.as_ref().unwrap()(&current[0..len], &n);
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

        let mut stream = RlpStream::new_list(17);
        let mut i = first_child_idx;
        for digit in 0..16 {
            if hash_mask & (1u16 << digit) != 0 {
                child_hashes.push(self.stack[i].to_vec());
                stream.append(&self.stack[i]);
                i = i + 1;
            } else {
                stream.append_empty_data();
            }
        }
        stream.append_empty_data();

        self.rlp_buffer = stream.out().to_vec();
        self.stack.truncate(first_child_idx);
        self.stack.push(node_ref(self.rlp_buffer.as_slice()));

        child_hashes
    }

    fn leaf_node_rlp(&mut self, path: &[u8], value: &[u8]) -> Vec<u8> {
        let encoded_path = encode_path(path, true);

        let mut stream = RlpStream::new_list(2);
        stream.append(&encoded_path);
        stream.append(&value);

        self.rlp_buffer = stream.out().to_vec();
        self.rlp_buffer.clone()
    }

    fn extension_node_rlp(&mut self, path: &[u8], child_ref: &[u8]) -> Vec<u8> {
        let encoded_path = encode_path(path, false);

        let mut stream = RlpStream::new_list(2);
        stream.append(&encoded_path);
        stream.append(&child_ref);

        self.rlp_buffer = stream.out().to_vec();
        self.rlp_buffer.clone()
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

fn unpack_nibbles(packed: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; packed.len() * 2];
    let mut i = 0;
    for b in packed {
        out[i] = b >> 4;
        out[i + 1] = b & 0x0F;
        i += 2;
    }
    out
}
