#![allow(clippy::question_mark)]
use crate::{
    kv::traits::{MutableCursor, Table},
    trie::{
        hash_builder::pack_nibbles,
        node::{unmarshal_node, Node},
        prefix_set::PrefixSet,
        util::has_prefix,
    },
};
use anyhow::Result;
use async_recursion::async_recursion;
use ethereum_types::H256;
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

        let first_nibbles_mask = (1u16 << self.nibble) - 1;
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
    C: MutableCursor<'tx, T>,
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
    C: MutableCursor<'tx, T>,
{
    async fn new(
        cursor: &'cu mut C,
        changed: &'ps mut PrefixSet,
        prefix: &[u8],
    ) -> Result<Cursor<'cu, 'tx, 'ps, C, T>> {
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

    async fn next(&mut self) -> Result<()> {
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
        let mut k = self.key();

        if self.can_skip_state && k.is_some() {
            k = increment_key(&k.unwrap());
        }
        if k.is_none() {
            return None;
        }

        Some(pack_nibbles(k.as_ref().unwrap()))
    }

    async fn consume_node(&mut self, to: &[u8], exact: bool) -> Result<()> {
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
            node = Some(unmarshal_node(entry.as_ref().unwrap().1.as_slice()).unwrap());
            assert_ne!(node.as_ref().unwrap().state_mask(), 0);
        }

        let mut nibble = 0i8;
        if node.is_none() || node.as_ref().unwrap().root_hash().is_some() {
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

    #[async_recursion(?Send)]
    async fn move_to_next_sibling(
        &mut self,
        allow_root_to_child_nibble_within_subnode: bool,
    ) -> Result<()> {
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

        if sn.node.is_none() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::{
            new_mem_database, tables,
            traits::{MutableKV, MutableTransaction},
        },
        trie::node::marshal_node,
    };
    use hex_literal::hex;

    #[tokio::test]
    async fn test_intermediate_hashes_cursor_traversal_1() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();
        let mut trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

        let key1 = vec![0x1u8];
        let node1 = Node::new(0b1011, 0b1001, 0, vec![], None);
        trie.upsert(key1, marshal_node(&node1)).await.unwrap();

        let key2 = vec![0x1u8, 0x0, 0xB];
        let node2 = Node::new(0b1010, 0, 0, vec![], None);
        trie.upsert(key2, marshal_node(&node2)).await.unwrap();

        let key3 = vec![0x1u8, 0x3];
        let node3 = Node::new(0b1110, 0, 0, vec![], None);
        trie.upsert(key3, marshal_node(&node3)).await.unwrap();

        let mut changed = PrefixSet::new();
        let mut cursor = Cursor::new(&mut trie, &mut changed, &[]).await.unwrap();

        assert!(cursor.key().unwrap().is_empty());

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0, 0xB, 0x1]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0, 0xB, 0x3]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x1]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x1]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x2]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x3]);

        cursor.next().await.unwrap();
        assert!(cursor.key().is_none());
    }

    #[tokio::test]
    async fn test_intermediate_hashes_cursor_traversal_2() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();
        let mut trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

        let key1 = vec![0x4u8];
        let node1 = Node::new(
            0b10100,
            0,
            0b00100,
            vec![H256::from(hex!(
                "0384e6e2c2b33c4eb911a08a7ff57f83dc3eb86d8d0c92ec112f3b416d6685a9"
            ))],
            None,
        );
        trie.upsert(key1, marshal_node(&node1)).await.unwrap();

        let key2 = vec![0x6u8];
        let node2 = Node::new(
            0b10010,
            0,
            0b00010,
            vec![H256::from(hex!(
                "7f9a58b00625a6e725559acf327baf88d90e4a5b65a2003acd24f110c0441df1"
            ))],
            None,
        );
        trie.upsert(key2, marshal_node(&node2)).await.unwrap();

        let mut changed = PrefixSet::new();
        let mut cursor = Cursor::new(&mut trie, &mut changed, &[]).await.unwrap();

        assert!(cursor.key().unwrap().is_empty());

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x4, 0x2]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x4, 0x4]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x6, 0x1]);

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x6, 0x4]);

        cursor.next().await.unwrap();
        assert!(cursor.key().is_none());
    }

    #[tokio::test]
    async fn test_intermediate_hashes_cursor_traversal_within_prefix() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();
        let mut trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

        let prefix_a = vec![0xau8, 0xa, 0x0, 0x2];
        let prefix_b = vec![0xbu8, 0xb, 0x0, 0x5];
        let prefix_c = vec![0xcu8, 0xc, 0x0, 0x1];

        let node_a = Node::new(
            0b10100,
            0,
            0,
            vec![],
            Some(H256::from(hex!(
                "2e1b81393448317fc1834241119c23f9e1763f7a662f8078949accc35b0d3b13"
            ))),
        );
        trie.upsert(prefix_a.clone(), marshal_node(&node_a))
            .await
            .unwrap();

        let node_b1 = Node::new(
            0b10100,
            0b00100,
            0,
            vec![],
            Some(H256::from(hex!(
                "c570b66136e99d07c6c6360769de1d9397805849879dd7c79cf0b8e6694bfb0e"
            ))),
        );
        trie.upsert(prefix_b.clone(), marshal_node(&node_b1))
            .await
            .unwrap();

        let node_b2 = Node::new(
            0b00010,
            0,
            0b00010,
            vec![H256::from(hex!(
                "6fc81f58df057a25ca6b687a6db54aaa12fbea1baf03aa3db44d499fb8a7af65"
            ))],
            None,
        );
        let mut key_b2 = prefix_b.clone();
        key_b2.push(0x2);
        trie.upsert(key_b2, marshal_node(&node_b2)).await.unwrap();

        let node_c = Node::new(
            0b11110,
            0,
            0,
            vec![],
            Some(H256::from(hex!(
                "0f12bed8e3cc4cce692d234e69a4d79c0e74ab05ecb808dad588212eab788c31"
            ))),
        );
        trie.upsert(prefix_c.clone(), marshal_node(&node_c))
            .await
            .unwrap();

        let mut changed = PrefixSet::new();
        let mut cursor = Cursor::new(&mut trie, &mut changed, prefix_b.as_slice())
            .await
            .unwrap();

        assert!(cursor.key().unwrap().is_empty());
        assert!(cursor.can_skip_state());

        cursor.next().await.unwrap();
        assert!(cursor.key().is_none());

        let mut changed = PrefixSet::new();
        changed.insert([prefix_b.as_slice(), &[0xdu8, 0x5]].concat().as_slice());
        changed.insert([prefix_c.as_slice(), &[0xbu8, 0x8]].concat().as_slice());
        let mut cursor = Cursor::new(&mut trie, &mut changed, prefix_b.as_slice())
            .await
            .unwrap();

        assert!(cursor.key().unwrap().is_empty());
        assert!(!cursor.can_skip_state());

        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), [0x2]);
        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), [0x2, 0x1]);
        cursor.next().await.unwrap();
        assert_eq!(cursor.key().unwrap(), [0x4]);

        cursor.next().await.unwrap();
        assert!(cursor.key().is_none());
    }

    #[test]
    fn test_intermediate_hashes_increment_key() {
        assert_eq!(increment_key(&[]), None);
        assert_eq!(increment_key(&[0x1, 0x2]), Some(vec![0x1, 0x3]));
        assert_eq!(increment_key(&[0x1, 0xF]), Some(vec![0x2, 0x0]));
        assert_eq!(increment_key(&[0xF, 0xF]), None);
        assert_eq!(increment_key(&[0x1, 0x2, 0x0]), Some(vec![0x1, 0x2, 0x1]));
        assert_eq!(increment_key(&[0x1, 0x2, 0xE]), Some(vec![0x1, 0x2, 0xF]));
        assert_eq!(increment_key(&[0x1, 0x2, 0xF]), Some(vec![0x1, 0x3, 0x0]));
        assert_eq!(increment_key(&[0x1, 0xF, 0xF]), Some(vec![0x2, 0x0, 0x0]));
        assert_eq!(increment_key(&[0xF, 0xF, 0xF]), None);
    }
}
