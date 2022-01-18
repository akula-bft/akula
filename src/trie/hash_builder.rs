use crate::trie::node::Node;
use ethereum_types::H256;
use std::boxed::Box;

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

type NodeCollector = Box<dyn Fn(&[u8], &Node) -> ()>;

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
        todo!()
    }

    fn add_leaf(&mut self, unpacked_key: Vec<u8>, value: &[u8]) {
        todo!()
    }

    fn add_branch_node(&mut self, unpacked_key: Vec<u8>, hash: &H256, is_in_db_trie: bool) {
        todo!()
    }

    fn root_hash(&mut self) -> H256 {
        todo!()
    }

    fn private_root_hash(&mut self, auto_finalize: bool) {
        todo!()
    }

    fn finalize(&mut self) {
        todo!()
    }

    fn gen_struct_step(&mut self, current: &[u8], succeeding: &[u8]) {
        todo!()
    }

    fn branch_ref(&mut self, state_mask: u16, hash_mask: u16) -> Vec<Vec<u8>> {
        todo!()
    }

    fn leaf_node_rlp(&mut self, path: &[u8], value: &[u8]) -> Vec<u8> {
        todo!()
    }

    fn extension_node_rlp(&mut self, path: &[u8], child_ref: &[u8]) -> Vec<u8> {
        todo!()
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
