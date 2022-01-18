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
    fn full_key(&self) -> Vec<u8> { todo!() }

    fn state_flag(&self) -> bool { todo!() }

    fn tree_flag(&self) -> bool { todo!() }

    fn hash_flag(&self) -> bool { todo!() }

    fn hash(&self) -> Option<H256> { todo!() }
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
        todo!();
    }

    fn next(&mut self) {
        todo!();
    }

    fn key(&self) -> Option<Vec<u8>> {
        todo!();
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