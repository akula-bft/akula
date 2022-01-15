#![allow(clippy::question_mark)]
use crate::{
    crypto::keccak256,
    etl::collector::{TableCollector, OPTIMAL_BUFFER_CAPACITY},
    kv::{
        tables,
        traits::{Cursor as _Cursor, CursorDupSort, MutableCursor, MutableTransaction, Table},
    },
    models::*,
    trie::{
        hash_builder::{pack_nibbles, unpack_nibbles, HashBuilder},
        node::{marshal_node, unmarshal_node, Node},
        prefix_set::PrefixSet,
        util::has_prefix,
    },
};
use anyhow::{bail, Result};
use async_recursion::async_recursion;
use std::{marker::PhantomData, sync::Mutex};
use tempfile::TempDir;
use tokio::sync::Mutex as AsyncMutex;

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
    cursor: AsyncMutex<&'cu mut C>,
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
            cursor: AsyncMutex::new(cursor),
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
            self.cursor.lock().await.seek_exact(db_key).await?
        } else {
            self.cursor.lock().await.seek(db_key).await?
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
            self.cursor.lock().await.delete_current().await?;
        }

        Ok(())
    }

    #[async_recursion]
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
            let key = self.key().clone();
            self.consume_node(key.as_ref().unwrap(), false).await?;
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

    fn changed_mut(&mut self) -> &mut PrefixSet {
        self.changed
    }
}

struct DbTrieLoader<'db, 'tx, 'tmp, 'co, 'nc, Tx>
where
    Tx: MutableTransaction<'db>,
    'db: 'tx,
    'tmp: 'co,
    'co: 'nc,
{
    txn: &'tx Tx,
    hb: HashBuilder<'nc>,
    storage_collector: Mutex<&'co mut TableCollector<'tmp, tables::TrieStorage>>,
    rlp: Vec<u8>,
    _marker: PhantomData<&'db ()>,
}

impl<'db, 'tx, 'tmp, 'co, 'nc, Tx> DbTrieLoader<'db, 'tx, 'tmp, 'co, 'nc, Tx>
where
    Tx: MutableTransaction<'db>,
    'db: 'tx,
    'tmp: 'co,
    'co: 'nc,
{
    fn new(
        txn: &'tx Tx,
        account_collector: &'co mut TableCollector<'tmp, tables::TrieAccount>,
        storage_collector: &'co mut TableCollector<'tmp, tables::TrieStorage>,
    ) -> Self {
        let mut instance = Self {
            txn,
            hb: HashBuilder::new(),
            storage_collector: Mutex::new(storage_collector),
            rlp: vec![],
            _marker: PhantomData,
        };

        let node_collector = |unpacked_key: &[u8], node: &Node| {
            if unpacked_key.is_empty() {
                return;
            }

            account_collector.push(unpacked_key.to_vec(), marshal_node(node));
        };

        instance.hb.node_collector = Some(Box::new(node_collector));

        instance
    }

    async fn calculate_root(&mut self, changed: &mut PrefixSet) -> Result<H256> {
        let mut state = self.txn.cursor(tables::HashedAccount).await?;
        let mut trie_db_cursor = self.txn.mutable_cursor(tables::TrieAccount).await?;

        let mut trie = Cursor::new(&mut trie_db_cursor, changed, &[]).await?;
        while trie.key().is_some() {
            if trie.can_skip_state() {
                assert!(trie.hash().is_some());
                self.hb.add_branch_node(
                    trie.key().unwrap(),
                    trie.hash().as_ref().unwrap(),
                    trie.children_are_in_trie(),
                );
            }

            let uncovered = trie.first_uncovered_prefix();
            if uncovered.is_none() {
                break;
            }

            trie.next().await?;

            let mut seek_key = uncovered.unwrap().to_vec();
            seek_key.resize(32, 0);

            let mut acc = state.seek(H256::from_slice(seek_key.as_slice())).await?;
            while acc.is_some() {
                let unpacked_key = unpack_nibbles(acc.unwrap().0.as_bytes());
                if trie.key().is_some() && trie.key().unwrap() < unpacked_key {
                    break;
                }

                let account = acc.unwrap().1;

                let storage_root = self
                    .calculate_storage_root(acc.unwrap().0.as_bytes(), trie.changed_mut())
                    .await?;

                self.hb.add_leaf(
                    unpacked_key,
                    rlp::encode(&account.to_rlp(storage_root)).as_ref(),
                );

                acc = state.next().await?
            }
        }

        Ok(self.hb.root_hash())
    }

    async fn calculate_storage_root(
        &self,
        key_with_inc: &[u8],
        changed: &mut PrefixSet,
    ) -> Result<H256> {
        let mut state = self.txn.cursor_dup_sort(tables::HashedStorage).await?;
        let mut trie_db_cursor = self.txn.mutable_cursor(tables::TrieStorage).await?;

        let mut hb = HashBuilder::new();
        hb.node_collector = Some(Box::new(|unpacked_storage_key: &[u8], node: &Node| {
            let key = [key_with_inc, unpacked_storage_key].concat();
            self.storage_collector
                .lock()
                .unwrap()
                .push(key, marshal_node(node));
        }));

        let mut trie = Cursor::new(&mut trie_db_cursor, changed, key_with_inc).await?;
        while trie.key().is_some() {
            if trie.can_skip_state() {
                assert!(trie.hash().is_some());
                hb.add_branch_node(
                    trie.key().unwrap(),
                    trie.hash().as_ref().unwrap(),
                    trie.children_are_in_trie(),
                );
            }

            let uncovered = trie.first_uncovered_prefix();
            if uncovered.is_none() {
                break;
            }

            trie.next().await?;

            let mut seek_key = uncovered.unwrap().to_vec();
            seek_key.resize(32, 0);

            let mut storage = state
                .seek_both_range(
                    H256::from_slice(key_with_inc),
                    H256::from_slice(seek_key.as_slice()),
                )
                .await?;
            while storage.is_some() {
                let (storage_location, value) = storage.unwrap();
                let unpacked_loc = unpack_nibbles(storage_location.as_bytes());
                if trie.key().is_some() && trie.key().unwrap() < unpacked_loc {
                    break;
                }
                let rlp = rlp::encode(&value);
                hb.add_leaf(unpacked_loc, rlp.as_ref());
                storage = state.next_dup().await?.map(|(_, v)| v);
            }
        }

        Ok(hb.root_hash())
    }
}

async fn do_increment_intermediate_hashes<'db, 'tx, Tx>(
    txn: &'tx Tx,
    etl_dir: &TempDir,
    expected_root: Option<H256>,
    changed: &mut PrefixSet,
) -> Result<H256>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    let mut account_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);

    let root = {
        let mut loader = DbTrieLoader::new(txn, &mut account_collector, &mut storage_collector);

        loader.calculate_root(changed).await?
    };

    if expected_root.is_some() && expected_root.unwrap() != root {
        bail!(
            "Wrong state root: expected {}, got {}",
            expected_root.unwrap(),
            root
        );
    }

    let mut target = txn.mutable_cursor(tables::TrieAccount.erased()).await?;
    account_collector.load(&mut target).await?;

    let mut target = txn.mutable_cursor(tables::TrieStorage.erased()).await?;
    storage_collector.load(&mut target).await?;

    Ok(root)
}

async fn gather_changes<'db, 'tx, Tx>(txn: &'tx Tx, from: BlockNumber) -> Result<PrefixSet>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    let starting_key = from + 1;

    let mut out = PrefixSet::new();

    let mut account_changes = txn.cursor_dup_sort(tables::AccountChangeSet).await?;
    let mut data = account_changes.seek(starting_key).await?;
    while data.is_some() {
        let address = data.unwrap().1.address;
        let hashed_address = keccak256(address);
        out.insert(unpack_nibbles(hashed_address.as_bytes()).as_slice());
        data = account_changes.next().await?;
    }

    let mut storage_changes = txn.cursor_dup_sort(tables::StorageChangeSet).await?;
    let mut data = storage_changes.seek(starting_key).await?;
    while data.is_some() {
        let address = data.as_ref().unwrap().0.address;
        let location = data.as_ref().unwrap().1.location;
        let hashed_address = keccak256(address);
        let hashed_location = keccak256(location);

        let hashed_key = [
            hashed_address.as_bytes(),
            unpack_nibbles(hashed_location.as_bytes()).as_slice(),
        ]
        .concat();
        out.insert(hashed_key.as_slice());
        data = storage_changes.next().await?;
    }

    Ok(out)
}

pub async fn increment_intermediate_hashes<'db, 'tx, Tx>(
    txn: &'tx Tx,
    etl_dir: &TempDir,
    from: BlockNumber,
    expected_root: Option<H256>,
) -> Result<H256>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    let mut changes = gather_changes(txn, from).await?;
    do_increment_intermediate_hashes(txn, etl_dir, expected_root, &mut changes).await
}

pub async fn regenerate_intermediate_hashes<'db, 'tx, Tx>(
    txn: &'tx Tx,
    etl_dir: &TempDir,
    expected_root: Option<H256>,
) -> Result<H256>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    txn.clear_table(tables::TrieAccount).await?;
    txn.clear_table(tables::TrieStorage).await?;
    let mut empty = PrefixSet::new();
    do_increment_intermediate_hashes(txn, etl_dir, expected_root, &mut empty).await
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

#[cfg(test)]
mod property_test {
    use super::*;
    use crate::{
        crypto::{keccak256, trie_root},
        h256_to_u256,
        kv::{
            new_mem_database, tables,
            tables::{AccountChange, StorageChange, StorageChangeKey},
            traits::{MutableCursor, MutableKV, MutableTransaction},
        },
        models::{Account, BlockNumber, EMPTY_ROOT},
        trie::regenerate_intermediate_hashes,
        u256_to_h256, zeroless_view,
    };
    use anyhow::Result;
    use proptest::prelude::*;
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    // strategies
    fn addresses() -> impl Strategy<Value = Address> {
        any::<[u8; 20]>().prop_map(Address::from)
    }

    fn h256s() -> impl Strategy<Value = H256> {
        any::<[u8; 32]>().prop_map(H256::from)
    }

    fn u256s() -> impl Strategy<Value = U256> {
        any::<[u8; 32]>().prop_map(|v| h256_to_u256(H256::from(v)))
    }

    fn nonzero_u256s() -> impl Strategy<Value = U256> {
        u256s().prop_filter("value must not be zero", |&x| x != 0)
    }

    prop_compose! {
        fn accounts()(
            nonce in any::<u64>(),
            balance in u256s(),
            code_hash in h256s(),
        ) -> Account {
            Account { nonce, balance, code_hash }
        }
    }

    type Storage = BTreeMap<H256, U256>;

    fn account_storages() -> impl Strategy<Value = Storage> {
        prop::collection::btree_map(h256s(), nonzero_u256s(), 0..20)
    }

    prop_compose! {
        fn accounts_with_storage()(
            account in accounts(),
            storage in account_storages(),
        ) -> (Account, Storage) {
            (account, storage)
        }
    }

    type ChangingAccount = BTreeMap<u32, Option<(Account, Storage)>>;

    #[derive(Debug)]
    struct ChangingAccountsFixture {
        accounts: BTreeMap<Address, ChangingAccount>,
        before_increment: u32,
        after_increment: u32,
    }

    fn changing_accounts(max_height: u32) -> impl Strategy<Value = ChangingAccount> {
        prop::collection::btree_map(
            0..max_height,
            prop::option::of(accounts_with_storage()),
            1..3,
        )
        .prop_filter("does not contain changes", |x| {
            for v in x.values() {
                if v.is_some() {
                    return true;
                }
            }
            false
        })
    }

    prop_compose! {
        fn test_datas()(
            after_increment in 2u32..,
        )(
            before_increment in 0..after_increment - 2,
            after_increment in Just(after_increment),
            accounts in prop::collection::btree_map(
                addresses(),
                changing_accounts(after_increment - 1),
                0..100
            ),
        ) -> ChangingAccountsFixture {
            ChangingAccountsFixture {
                accounts,
                before_increment,
                after_increment,
            }
        }
    }

    // helper functions
    fn expected_storage_root(storage: &Storage) -> H256 {
        if storage.is_empty() {
            EMPTY_ROOT
        } else {
            trie_root(storage.iter().map(|(k, v)| {
                (
                    keccak256(k.to_fixed_bytes()),
                    rlp::encode(&zeroless_view(&u256_to_h256(*v))),
                )
            }))
        }
    }

    fn expected_state_root(accounts_with_storage: &BTreeMap<Address, (Account, Storage)>) -> H256 {
        trie_root(
            accounts_with_storage
                .iter()
                .map(|(&address, (account, storage))| {
                    let account_rlp = account.to_rlp(expected_storage_root(storage));
                    (keccak256(address), rlp::encode(&account_rlp))
                }),
        )
    }

    fn accounts_at_height(
        changing_accounts: &ChangingAccountsFixture,
        height: u32,
    ) -> BTreeMap<Address, (Account, Storage)> {
        let mut result = BTreeMap::new();
        for (address, state) in &changing_accounts.accounts {
            if let Some(account_with_storage) = changing_account_at_height(state, height) {
                result.insert(*address, account_with_storage.clone());
            }
        }
        result
    }

    async fn add_account_to_hashed_state<'tx, 'cu, AC, SC>(
        account_cursor: &'cu mut AC,
        storage_cursor: &'cu mut SC,
        address: &Address,
        account: &Account,
        storage: &Storage,
    ) -> Result<()>
    where
        'tx: 'cu,
        AC: MutableCursor<'tx, tables::HashedAccount>,
        SC: MutableCursor<'tx, tables::HashedStorage>,
    {
        let address_hash = keccak256(address);
        account_cursor.upsert(address_hash, *account).await?;
        for (location, value) in storage {
            let location_hash = keccak256(location);
            storage_cursor
                .upsert(address_hash, (location_hash, *value))
                .await?
        }
        Ok(())
    }

    async fn populate_hashed_state<'db, 'tx, Tx>(
        tx: &'tx Tx,
        accounts_with_storage: BTreeMap<Address, (Account, Storage)>,
    ) -> Result<()>
    where
        'db: 'tx,
        Tx: MutableTransaction<'db>,
    {
        tx.clear_table(tables::HashedAccount).await?;
        tx.clear_table(tables::HashedStorage).await?;

        let mut account_cursor = tx.mutable_cursor(tables::HashedAccount).await?;
        let mut storage_cursor = tx.mutable_cursor_dupsort(tables::HashedStorage).await?;

        for (address, (account, storage)) in accounts_with_storage {
            add_account_to_hashed_state(
                &mut account_cursor,
                &mut storage_cursor,
                &address,
                &account,
                &storage,
            )
            .await?;
        }

        Ok(())
    }

    async fn populate_change_sets<'db, 'tx, Tx>(
        tx: &'tx Tx,
        changing_accounts: &BTreeMap<Address, ChangingAccount>,
    ) -> Result<()>
    where
        'db: 'tx,
        Tx: MutableTransaction<'db>,
    {
        tx.clear_table(tables::AccountChangeSet).await?;
        tx.clear_table(tables::StorageChangeSet).await?;

        let mut account_cursor = tx.mutable_cursor_dupsort(tables::AccountChangeSet).await?;
        let mut storage_cursor = tx.mutable_cursor_dupsort(tables::StorageChangeSet).await?;

        for (address, states) in changing_accounts {
            let mut previous: Option<&(Account, Storage)> = None;
            for (height, current) in states {
                let block_number = BlockNumber(*height as u64);
                if current.as_ref() != previous {
                    let previous_account = previous.as_ref().map(|(a, _)| *a);
                    let current_account = current.as_ref().map(|(a, _)| *a);
                    if current_account != previous_account {
                        account_cursor
                            .upsert(
                                block_number,
                                AccountChange {
                                    address: *address,
                                    account: previous_account,
                                },
                            )
                            .await?;
                    }
                    let empty_storage = Storage::new();
                    let previous_storage =
                        previous.as_ref().map(|(_, s)| s).unwrap_or(&empty_storage);
                    let current_storage =
                        current.as_ref().map(|(_, s)| s).unwrap_or(&empty_storage);
                    for (location, value) in previous_storage {
                        if current_storage.get(location).unwrap_or(&U256::ZERO) != value {
                            storage_cursor
                                .upsert(
                                    StorageChangeKey {
                                        block_number,
                                        address: *address,
                                    },
                                    StorageChange {
                                        location: *location,
                                        value: *value,
                                    },
                                )
                                .await?;
                        }
                    }
                    for location in current_storage.keys() {
                        if !previous_storage.contains_key(location) {
                            storage_cursor
                                .upsert(
                                    StorageChangeKey {
                                        block_number,
                                        address: *address,
                                    },
                                    StorageChange {
                                        location: *location,
                                        value: U256::ZERO,
                                    },
                                )
                                .await?;
                        }
                    }
                }
                previous = current.as_ref();
            }
        }

        Ok(())
    }

    fn changing_account_at_height(
        account: &ChangingAccount,
        height: u32,
    ) -> Option<&(Account, Storage)> {
        for (changed_at, state) in account.iter().rev() {
            if changed_at <= &height {
                return state.as_ref();
            }
        }
        None
    }

    // test
    async fn do_trie_root_matches(test_data: ChangingAccountsFixture) {
        let db = new_mem_database().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let tx = db.begin_mutable().await.unwrap();
        let state_before_increment = accounts_at_height(&test_data, test_data.before_increment);
        let expected = expected_state_root(&state_before_increment);
        populate_hashed_state(&tx, state_before_increment)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let tx = db.begin_mutable().await.unwrap();
        let root = regenerate_intermediate_hashes(&tx, &temp_dir, None)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        assert_eq!(root, expected);

        let tx = db.begin_mutable().await.unwrap();
        let state_after_increment = accounts_at_height(&test_data, test_data.after_increment);
        let expected = expected_state_root(&state_after_increment);
        populate_hashed_state(&tx, state_after_increment)
            .await
            .unwrap();
        populate_change_sets(&tx, &test_data.accounts)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let tx = db.begin_mutable().await.unwrap();
        let root = increment_intermediate_hashes(
            &tx,
            &temp_dir,
            BlockNumber(test_data.before_increment as u64),
            None,
        )
        .await
        .unwrap();

        assert_eq!(root, expected);
    }

    proptest! {
        #[test]
        fn trie_root_matches(test_data in test_datas()) {
            tokio_test::block_on(do_trie_root_matches(test_data));
        }
    }
}
