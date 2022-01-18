use crate::{
    kv::traits::{MutableCursor, Table, TableDecode, Transaction},
    trie::{
        hash_builder::{pack_nibbles, HashBuilder},
        node::{Node, unmarshal_node},
        prefix_set::PrefixSet,
    },
};
use anyhow::Result;
use async_recursion::async_recursion;
use ethereum_types::H256;
use std::marker::{PhantomData, Sync};
use crate::trie::util::has_prefix;

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

fn increment_key(unpacked: &[u8]) -> Option<Vec<u8>> {
    let mut out = unpacked.to_vec();

    for i in (0..out.len()).rev() {
        let nibble = out[i];
        assert!(nibble < 0x10);
        if nibble < 0xf {
            out[i] += 1;
            return Some(out);
        } else {
            out[i] = 0;
        }
    }

    None
}

struct Cursor<'cu, 'tx, 'ps, C, T>
where
    T: Table<Key = Vec<u8>, SeekKey = Vec<u8>, Value = Vec<u8>>,
    'tx: 'cu,
    C: MutableCursor<'tx, T> + Sync,
{
    cursor: &'cu mut C,
    changed: &'ps mut PrefixSet,
    prefix: Vec<u8>,
    stack: Vec<CursorSubNode>,
    can_skip_state: bool,
    _marker: PhantomData<&'tx T>,
}

impl<'cu, 'tx, 'ps, C, T> Cursor<'cu, 'tx, 'ps, C, T>
where
    T: Table<Key = Vec<u8>, SeekKey = Vec<u8>, Value = Vec<u8>>,
    'tx: 'cu,
    C: MutableCursor<'tx, T> + Sync,
{
    async fn new(
        cursor: &'cu mut C,
        changed: &'ps mut PrefixSet,
        prefix: &[u8],
    ) -> anyhow::Result<Cursor<'cu, 'tx, 'ps, C, T>> {
        let mut new_cursor = Self {
            cursor,
            changed,
            prefix: prefix.to_vec(),
            stack: vec![],
            can_skip_state: false,
            _marker: PhantomData,
        };
        new_cursor.consume_node(&[], true).await?;
        Ok(new_cursor)
    }

    async fn next(&mut self) -> anyhow::Result<()> {
        if self.stack.is_empty() {
            return Ok(()); // end-of-tree
        }

        if !self.can_skip_state && self.children_are_in_trie() {
            if self.stack.last().unwrap().nibble < 0 {
                self.move_to_next_sibling(true).await?;
            } else {
                self.consume_node(&self.key().unwrap(), false).await?;
            }
        } else {
            self.move_to_next_sibling(false).await?;
        }

        self.update_skip_state();
        Ok(())
    }

    fn key(&self) -> Option<Vec<u8>> {
        if self.stack.is_empty() {
            None
        } else {
            Some(self.stack.last().unwrap().full_key())
        }
    }

    fn hash(&self) -> Option<H256> {
        if self.stack.is_empty() {
            return None;
        }
        self.stack.last().unwrap().hash()
    }

    fn children_are_in_trie(&self) -> bool {
        if self.stack.is_empty() {
            return false;
        }
        self.stack.last().unwrap().tree_flag()
    }

    fn can_skip_state(&self) -> bool {
        self.can_skip_state
    }

    fn first_uncovered_prefix(&self) -> Option<Vec<u8>> {
        let mut k = self.key().clone();

        if self.can_skip_state && k.is_some() {
            k = increment_key(&k.unwrap());
        }
        if k.is_none() {
            return None;
        }

        Some(pack_nibbles(k.as_ref().unwrap()))
    }

    async fn consume_node(&mut self, to: &[u8], exact: bool) -> anyhow::Result<()> {
        let db_key = [self.prefix.as_slice(), to].concat().to_vec();
        let entry = if exact {
            self.cursor.seek_exact(db_key).await?
        } else {
            self.cursor.seek(db_key).await?
        };

        if entry.is_none() && !exact {
            self.stack.clear();
            return Ok(());
        }

        let mut key = to.to_vec();
        if !exact {
            key = entry.as_ref().unwrap().0.clone();
            if !has_prefix(key.as_slice(), self.prefix.as_slice()) {
                self.stack.clear();
                return Ok(());
            }
            key.drain(0..self.prefix.len());
        }

        let mut node: Option<Node> = None;
        if entry.is_some() {
            let node = unmarshal_node(entry.as_ref().unwrap().1.as_slice()).unwrap();
            assert_ne!(node.state_mask(), 0);
        }

        let mut nibble = 0i8;
        if !node.is_some() || node.as_ref().unwrap().root_hash().is_some() {
            nibble = -1;
        } else {
            while node.as_ref().unwrap().state_mask() & (1u16 << nibble) == 0 {
                nibble += 1;
            }
        }

        if !key.is_empty() && !self.stack.is_empty() {
            self.stack[0].nibble = key[0] as i8;
        }
        self.stack.push(CursorSubNode { key, node, nibble });

        self.update_skip_state();

        if entry.is_some() && (!self.can_skip_state || nibble != -1) {
            self.cursor.delete_current().await?;
        }

        Ok(())
    }

    #[async_recursion]
    async fn move_to_next_sibling(
        &mut self,
        allow_root_to_child_nibble_within_subnode: bool,
    ) -> anyhow::Result<()> {
        if self.stack.is_empty() {
            return Ok(());
        }

        let sn = self.stack.last().unwrap();

        if sn.nibble >= 15 || (sn.nibble < 0 && !allow_root_to_child_nibble_within_subnode) {
            self.stack.pop();
            self.move_to_next_sibling(false).await?;
            return Ok(());
        }

        let sn = self.stack.last_mut().unwrap();

        sn.nibble += 1;

        if !sn.node.is_some() {
            self.consume_node(self.key().as_ref().unwrap(), false)
                .await?;
            return Ok(());
        }

        while sn.nibble < 16 {
            if sn.state_flag() {
                return Ok(());
            }
            sn.nibble += 1;
        }

        self.stack.pop();
        self.move_to_next_sibling(false).await?;
        Ok(())
    }

    fn update_skip_state(&mut self) {
        if self.key().is_none()
            || self.changed.contains(
                [self.prefix.as_slice(), self.key().unwrap().as_slice()]
                    .concat()
                    .as_slice(),
            )
        {
            self.can_skip_state = false;
        } else {
            self.can_skip_state = self.stack.last().unwrap().hash_flag();
        }
    }
}

struct DbTrieLoader;

impl DbTrieLoader {}

pub fn regenerate_intermediate_hashes() {
    todo!()
}

pub fn increment_intermediate_hashes() {
    todo!()
}
