use ethereum_types::H256;
use crate::{
    kv::traits::{Cursor as MdbxCursor, Table, Transaction},
    trie::{hash_builder::HashBuilder, node::Node, prefix_set::PrefixSet},
};
use std::marker::PhantomData;

struct CursorSubNode {
    key: Vec<u8>,
    node: Option<Node>,
    nibble: i8,
}

impl CursorSubNode {
    fn full_key(&self) -> Vec<u8> {
        let mut out = self.key.clone();
        if self.nibble >= 0 {
            out.push(self.nibble as u8)
        }
        out
    }

    fn state_flag(&self) -> bool {
        if self.nibble < 0 || self.node.is_none() {
            return true;
        }
        self.node.as_ref().unwrap().state_mask() & (1u16 << self.nibble) != 0
    }

    fn tree_flag(&self) -> bool {
        if self.nibble < 0 || self.node.is_none() {
            return true;
        }
        self.node.as_ref().unwrap().tree_mask() & (1u16 << self.nibble) != 0
    }

    fn hash_flag(&self) -> bool {
        if self.node.is_none() {
            return false;
        } else if self.nibble < 0 {
            return self.node.as_ref().unwrap().root_hash().is_some();
        }
        self.node.as_ref().unwrap().hash_mask() & (1u16 << self.nibble) != 0
    }

    fn hash(&self) -> Option<H256> {
        if !self.hash_flag() {
            return None;
        }

        if self.nibble < 0 {
            return self.node.as_ref().unwrap().root_hash();
        }

        let first_nibbles_mask = 1u16 << self.nibble - 1;
        let hash_idx = (self.node.as_ref().unwrap().hash_mask() & first_nibbles_mask).count_ones();
        Some(self.node.as_ref().unwrap().hashes()[hash_idx as usize])
    }
}

struct Cursor<'cu, 'tx, 'ps, C, T>
    where
        T: Table,
        'tx: 'cu,
        C: MdbxCursor<'tx, T>
{
    cursor: &'cu C,
    changed: &'ps PrefixSet,
    prefix: Vec<u8>,
    stack: Vec<CursorSubNode>,
    can_skip_state: bool,
    _marker: PhantomData<&'tx T>,
}

impl<'cu, 'tx, 'ps, C, T> Cursor<'cu, 'tx, 'ps, C, T>
    where
        T: Table,
        'tx: 'cu,
        C: MdbxCursor <'tx, T>
{
    fn new(cursor: &'cu C, changed: &'ps PrefixSet, prefix: &[u8]) -> Self {
        let mut new_cursor = Self {
            cursor,
            changed,
            prefix: prefix.to_vec(),
            stack: vec![],
            can_skip_state: false,
            _marker: PhantomData,
        };
        new_cursor.consume_node(&[], true);
        new_cursor
    }

    fn next(&mut self) {
        if self.stack.is_empty() {
            return; // end-of-tree
        }

        if !self.can_skip_state && self.children_are_in_trie() {
            if self.stack.last().unwrap().nibble < 0 {
                self.move_to_next_sibling(true);
            } else {
                self.consume_node(&self.key().unwrap(),false);
            }
        } else {
            self.move_to_next_sibling(false);
        }

        self.update_skip_state();
    }

    fn key(&self) -> Option<Vec<u8>> {
        if self.stack.is_empty() {
            None
        } else {
            Some(self.stack.last().unwrap().full_key())
        }
    }

    fn hash(&self) -> H256 {
        todo!();
    }

    fn children_are_in_trie(&self) -> bool {
        todo!();
    }

    fn can_skip_state(&self) -> bool {
        self.can_skip_state
    }

    fn first_uncovered_prefix(&self) -> Option<Vec<u8>> {
        todo!();
    }

    fn consume_node(&mut self, key: &[u8], exact: bool) {
        todo!();
    }

    fn move_to_next_sibling(&mut self, allow_root_to_child_nibble_within_subnode: bool) {
        todo!();
    }

    fn update_skip_state(&mut self) {
        todo!();
    }
}

struct DbTrieLoader;

impl DbTrieLoader {}

pub fn regenerate_intermediate_hashes() { todo!() }

pub fn increment_intermediate_hashes() { todo!() }