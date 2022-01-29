#![allow(clippy::question_mark)]
use crate::{
    crypto::keccak256,
    etl::collector::{TableCollector, OPTIMAL_BUFFER_CAPACITY},
    kv::{
        tables,
        traits::{Cursor as _Cursor, *},
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
    use std::collections::HashMap;

    use super::*;
    use crate::{
        kv::{new_mem_database, tables},
        trie::node::marshal_node,
        u256_to_h256, upsert_hashed_storage_value,
    };
    use hex_literal::hex;
    use maplit::hashmap;
    use tokio_stream::StreamExt;

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

    #[tokio::test]
    async fn cursor_traversal_within_prefix() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let mut trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();

        let prefix_a: [u8; 4] = [0xa, 0xa, 0x0, 0x2];
        let prefix_b: [u8; 4] = [0xb, 0xb, 0x0, 0x5];
        let prefix_c: [u8; 4] = [0xc, 0xc, 0x0, 0x1];

        let node_a = Node::new(
            0b10100,
            0,
            0,
            vec![],
            Some(hex!("2e1b81393448317fc1834241119c23f9e1763f7a662f8078949accc35b0d3b13").into()),
        );
        trie.upsert(prefix_a.to_vec(), marshal_node(&node_a))
            .await
            .unwrap();

        let node_b1 = Node::new(
            0b10100,
            0b00100,
            0,
            vec![],
            Some(hex!("c570b66136e99d07c6c6360769de1d9397805849879dd7c79cf0b8e6694bfb0e").into()),
        );
        let node_b2 = Node::new(
            0b00010,
            0,
            0b00010,
            vec![hex!("6fc81f58df057a25ca6b687a6db54aaa12fbea1baf03aa3db44d499fb8a7af65").into()],
            None,
        );
        trie.upsert(prefix_b.to_vec(), marshal_node(&node_b1))
            .await
            .unwrap();
        trie.upsert(
            [&prefix_b as &[u8], &([0x2] as [u8; 1]) as &[u8]].concat(),
            marshal_node(&node_b2),
        )
        .await
        .unwrap();

        let node_c = Node::new(
            0b11110,
            0,
            0,
            vec![],
            Some(hex!("0f12bed8e3cc4cce692d234e69a4d79c0e74ab05ecb808dad588212eab788c31").into()),
        );
        trie.upsert(prefix_c.to_vec(), marshal_node(&node_c))
            .await
            .unwrap();

        // No changes
        let mut changed = PrefixSet::new();
        let mut cursor = Cursor::new(&mut trie, &mut changed, &prefix_b)
            .await
            .unwrap();

        assert_eq!(cursor.key(), Some(vec![])); // root
        assert!(cursor.can_skip_state()); // due to root_hash
        cursor.next().await.unwrap(); // skips to end of trie
        assert_eq!(cursor.key(), None);

        // Some changes
        let mut changed = PrefixSet::new();
        changed.insert(&[&prefix_b as &[u8], &([0xD, 0x5] as [u8; 2]) as &[u8]].concat());
        changed.insert(&[&prefix_c as &[u8], &([0xB, 0x8] as [u8; 2]) as &[u8]].concat());
        let mut cursor = Cursor::new(&mut trie, &mut changed, &prefix_b)
            .await
            .unwrap();

        assert_eq!(cursor.key(), Some(vec![])); // root
        assert!(!cursor.can_skip_state());
        cursor.next().await.unwrap();
        assert_eq!(cursor.key(), Some(vec![0x2]));
        cursor.next().await.unwrap();
        assert_eq!(cursor.key(), Some(vec![0x2, 0x1]));
        cursor.next().await.unwrap();
        assert_eq!(cursor.key(), Some(vec![0x4]));

        cursor.next().await.unwrap();
        assert_eq!(cursor.key(), None); // end of trie
    }

    async fn setup_storage<'db, Tx: MutableTransaction<'db>>(tx: &Tx, storage_key: H256) -> H256 {
        let mut hashed_storage = tx
            .mutable_cursor_dupsort(tables::HashedStorage)
            .await
            .unwrap();

        let mut hb = HashBuilder::new();

        for (loc, val) in [
            (
                hex!("1200000000000000000000000000000000000000000000000000000000000000"),
                0x42_u128,
            ),
            (
                hex!("1400000000000000000000000000000000000000000000000000000000000000"),
                0x01_u128,
            ),
            (
                hex!("3000000000000000000000000000000000000000000000000000000000E00000"),
                0x127a89_u128,
            ),
            (
                hex!("3000000000000000000000000000000000000000000000000000000000E00001"),
                0x05_u128,
            ),
        ] {
            let loc = H256(loc);
            let val = val.as_u256();

            upsert_hashed_storage_value(&mut hashed_storage, storage_key, loc, val)
                .await
                .unwrap();

            hb.add_leaf(unpack_nibbles(loc.as_bytes()), &rlp::encode(&val));
        }

        hb.root_hash()
    }

    async fn read_all_nodes<
        'tx,
        C: crate::kv::traits::Cursor<'tx, T>,
        T: Table<Key = Vec<u8>, Value = Vec<u8>>,
    >(
        cursor: &mut C,
    ) -> HashMap<Vec<u8>, Node> {
        walk(cursor, None)
            .map(|res| {
                let (k, v) = res.unwrap();
                (k, unmarshal_node(&v).unwrap())
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect()
    }

    #[tokio::test]
    async fn account_and_storage_trie() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
        let mut hb = HashBuilder::new();

        let key1 = hex!("B000000000000000000000000000000000000000000000000000000000000000").into();
        let a1 = Account {
            nonce: 0,
            balance: 3.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key1, a1).await.unwrap();
        hb.add_leaf(
            unpack_nibbles(&key1[..]),
            &rlp::encode(&a1.to_rlp(EMPTY_ROOT)),
        );

        // Some address whose hash starts with 0xB040
        let address2: Address = hex!("7db3e81b72d2695e19764583f6d219dbee0f35ca").into();
        let key2 = keccak256(address2);
        assert_eq!(key2[0], 0xB0);
        assert_eq!(key2[1], 0x40);
        let a2 = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key2, a2).await.unwrap();
        hb.add_leaf(
            unpack_nibbles(&key2[..]),
            &rlp::encode(&a2.to_rlp(EMPTY_ROOT)),
        );

        // Some address whose hash starts with 0xB041
        let address3: Address = hex!("16b07afd1c635f77172e842a000ead9a2a222459").into();
        let key3 = keccak256(address3);
        assert!((key3[0] == 0xB0 && key3[1] == 0x41));
        let code_hash =
            hex!("5be74cad16203c4905c068b012a2e9fb6d19d036c410f16fd177f337541440dd").into();
        let a3 = Account {
            nonce: 0,
            balance: 2.as_u256() * ETHER,
            code_hash,
        };
        hashed_accounts.upsert(key3, a3).await.unwrap();

        let storage_root = setup_storage(&txn, key3).await;

        hb.add_leaf(
            unpack_nibbles(&key3[..]),
            &rlp::encode(&a3.to_rlp(storage_root)),
        );

        let key4a = hex!("B1A0000000000000000000000000000000000000000000000000000000000000").into();
        let a4a = Account {
            nonce: 0,
            balance: 4.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key4a, a4a).await.unwrap();
        hb.add_leaf(
            unpack_nibbles(&key4a[..]),
            &rlp::encode(&a4a.to_rlp(EMPTY_ROOT)),
        );

        let key5 = hex!("B310000000000000000000000000000000000000000000000000000000000000").into();
        let a5 = Account {
            nonce: 0,
            balance: 8.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key5, a5).await.unwrap();
        hb.add_leaf(
            unpack_nibbles(&key5[..]),
            &rlp::encode(&a5.to_rlp(EMPTY_ROOT)),
        );

        let key6 = hex!("B340000000000000000000000000000000000000000000000000000000000000").into();
        let a6 = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key6, a6).await.unwrap();
        hb.add_leaf(
            unpack_nibbles(&key6[..]),
            &rlp::encode(&a6.to_rlp(EMPTY_ROOT)),
        );

        // ----------------------------------------------------------------
        // Populate account & storage trie DB tables
        // ----------------------------------------------------------------

        regenerate_intermediate_hashes(&txn, &temp_dir, Some(hb.root_hash()))
            .await
            .unwrap();

        // ----------------------------------------------------------------
        // Check account trie
        // ----------------------------------------------------------------

        let mut account_trie = txn.cursor(tables::TrieAccount).await.unwrap();

        let node_map = read_all_nodes(&mut account_trie).await;
        assert_eq!(node_map.len(), 2);

        let node1a = &node_map[&vec![0xB]];

        assert_eq!(node1a.state_mask(), 0b1011);
        assert_eq!(node1a.tree_mask(), 0b0001);
        assert_eq!(node1a.hash_mask(), 0b1001);

        assert_eq!(node1a.root_hash(), None);
        assert_eq!(node1a.hashes().len(), 2);

        let node2a = &node_map[&vec![0xB, 0x0]];

        assert_eq!(node2a.state_mask(), 0b10001);
        assert_eq!(node2a.tree_mask(), 0b00000);
        assert_eq!(node2a.hash_mask(), 0b10000);

        assert_eq!(node2a.root_hash(), None);
        assert_eq!(node2a.hashes().len(), 1);

        // ----------------------------------------------------------------
        // Check storage trie
        // ----------------------------------------------------------------

        let mut storage_trie = txn.cursor(tables::TrieStorage).await.unwrap();

        let node_map = read_all_nodes(&mut storage_trie).await;
        assert_eq!(node_map.len(), 1);

        let node3 = &node_map[&key3.0.to_vec()];

        assert_eq!(node3.state_mask(), 0b1010);
        assert_eq!(node3.tree_mask(), 0b0000);
        assert_eq!(node3.hash_mask(), 0b0010);

        assert_eq!(node3.root_hash(), Some(storage_root));
        assert_eq!(node3.hashes().len(), 1);

        // ----------------------------------------------------------------
        // Add an account
        // ----------------------------------------------------------------

        // Some address whose hash starts with 0xB1
        let address4b = hex!("4f61f2d5ebd991b85aa1677db97307caf5215c91").into();
        let key4b = keccak256(address4b);
        assert_eq!(key4b.0[0], key4a.0[0]);

        hashed_accounts
            .upsert(
                key4b,
                Account {
                    nonce: 0,
                    balance: 5.as_u256() * ETHER,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut account_change_table = txn.mutable_cursor(tables::AccountChangeSet).await.unwrap();
        account_change_table
            .upsert(
                BlockNumber(1),
                tables::AccountChange {
                    address: address4b,
                    account: None,
                },
            )
            .await
            .unwrap();

        increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None)
            .await
            .unwrap();

        let node_map = read_all_nodes(&mut account_trie).await;
        assert_eq!(node_map.len(), 2);

        let node1b = &node_map[&vec![0xB]];
        assert_eq!(node1b.state_mask(), 0b1011);
        assert_eq!(node1b.tree_mask(), 0b0001);
        assert_eq!(node1b.hash_mask(), 0b1011);

        assert_eq!(node1b.root_hash(), None);

        assert_eq!(node1b.hashes().len(), 3);
        assert_eq!(node1a.hashes()[0], node1b.hashes()[0]);
        assert_eq!(node1a.hashes()[1], node1b.hashes()[2]);

        let node2b = &node_map[&vec![0xB, 0x0]];
        assert_eq!(node2a, node2b);

        drop(hashed_accounts);
        drop(account_change_table);
        drop(account_trie);
        drop(storage_trie);
        txn.commit().await.unwrap();

        // Delete an account
        {
            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
            let mut account_trie = txn.cursor(tables::TrieAccount).await.unwrap();
            let mut account_change_table =
                txn.mutable_cursor(tables::AccountChangeSet).await.unwrap();
            {
                let account = hashed_accounts.seek_exact(key2).await.unwrap().unwrap().1;
                hashed_accounts.delete_current().await.unwrap();
                account_change_table
                    .upsert(
                        BlockNumber(2),
                        tables::AccountChange {
                            address: address2,
                            account: Some(account),
                        },
                    )
                    .await
                    .unwrap();
            }

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(1), None)
                .await
                .unwrap();

            let node_map = read_all_nodes(&mut account_trie).await;
            assert_eq!(node_map.len(), 1);

            let node1c = &node_map[&vec![0xB]];
            assert_eq!(node1c.state_mask(), 0b1011);
            assert_eq!(node1c.tree_mask(), 0b0000);
            assert_eq!(node1c.hash_mask(), 0b1011);

            assert_eq!(node1c.root_hash(), None);

            assert_eq!(node1c.hashes().len(), 3);
            assert_ne!(node1b.hashes()[0], node1c.hashes()[0]);
            assert_eq!(node1b.hashes()[1], node1c.hashes()[1]);
            assert_eq!(node1b.hashes()[2], node1c.hashes()[2]);
        }

        // Delete several accounts
        {
            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
            let mut account_trie = txn.cursor(tables::TrieAccount).await.unwrap();
            let mut account_change_table =
                txn.mutable_cursor(tables::AccountChangeSet).await.unwrap();
            for (key, address) in [(key2, address2), (key3, address3)] {
                let account = hashed_accounts.seek_exact(key).await.unwrap().unwrap().1;
                hashed_accounts.delete_current().await.unwrap();
                account_change_table
                    .upsert(
                        BlockNumber(2),
                        tables::AccountChange {
                            address,
                            account: Some(account),
                        },
                    )
                    .await
                    .unwrap();
            }

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(1), None)
                .await
                .unwrap();

            assert_eq!(
                read_all_nodes(&mut account_trie).await,
                hashmap! {
                    vec![0xB] => Node::new(0b1011, 0b0000, 0b1010, vec![node1b.hashes()[1], node1b.hashes()[2]], None)
                }
            );
        }
    }

    #[tokio::test]
    async fn account_trie_around_extension_node() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let a = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };

        let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
        let mut hb = HashBuilder::new();

        for key in [
            hex!("30af561000000000000000000000000000000000000000000000000000000000"),
            hex!("30af569000000000000000000000000000000000000000000000000000000000"),
            hex!("30af650000000000000000000000000000000000000000000000000000000000"),
            hex!("30af6f0000000000000000000000000000000000000000000000000000000000"),
            hex!("30af8f0000000000000000000000000000000000000000000000000000000000"),
            hex!("3100000000000000000000000000000000000000000000000000000000000000"),
        ] {
            hashed_accounts.upsert(H256(key), a).await.unwrap();
            hb.add_leaf(
                unpack_nibbles(&key[..]),
                &rlp::encode(&a.to_rlp(EMPTY_ROOT)),
            );
        }

        let expected_root = hb.root_hash();
        assert_eq!(
            regenerate_intermediate_hashes(&txn, &temp_dir, Some(expected_root))
                .await
                .unwrap(),
            expected_root
        );

        let mut account_trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

        let node_map = read_all_nodes(&mut account_trie).await;
        assert_eq!(node_map.len(), 2);

        assert_eq!(
            node_map[&vec![0x3]],
            Node::new(0b11, 0b01, 0b00, vec![], None)
        );

        let node2 = &node_map[&vec![0x3, 0x0, 0xA, 0xF]];

        assert_eq!(node2.state_mask(), 0b101100000);
        assert_eq!(node2.tree_mask(), 0b000000000);
        assert_eq!(node2.hash_mask(), 0b001000000);

        assert_eq!(node2.root_hash(), None);
        assert_eq!(node2.hashes().len(), 1);
    }

    fn int_to_address(i: u128) -> Address {
        let mut address = Address::zero();
        address[4..].copy_from_slice(&i.to_be_bytes());
        address
    }

    #[tokio::test]
    async fn incremental_vs_regeneration() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_database().unwrap();

        const N: u128 = 10_000;
        let one_eth = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };
        let two_eth = Account {
            nonce: 0,
            balance: 2.as_u256() * ETHER,
            ..Default::default()
        };

        let (incremental_root, incremental_nodes) = {
            // ------------------------------------------------------------------------------
            // Take A: create some accounts at genesis and then apply some changes at Block 1
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
            let mut account_change_table =
                txn.mutable_cursor(tables::AccountChangeSet).await.unwrap();
            let mut account_trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

            // Start with 3n accounts at genesis, each holding 1 ETH
            for i in 0..3 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).await.unwrap();
            }

            regenerate_intermediate_hashes(&txn, &temp_dir, None)
                .await
                .unwrap();

            let block_key = BlockNumber(1);

            // Double the balance of the first third of the accounts
            for i in 0..N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, two_eth).await.unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: Some(one_eth),
                        },
                    )
                    .await
                    .unwrap();
            }

            // Delete the second third of the accounts
            for i in N..2 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                let account = hashed_accounts.seek_exact(hash).await.unwrap().unwrap().1;
                hashed_accounts.delete_current().await.unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: Some(account),
                        },
                    )
                    .await
                    .unwrap();
            }

            // Don't touch the last third of genesis accounts

            // And add some new accounts, each holding 1 ETH
            for i in 3 * N..4 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).await.unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: None,
                        },
                    )
                    .await
                    .unwrap();
            }

            let incremental_root =
                increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None)
                    .await
                    .unwrap();

            let incremental_nodes = read_all_nodes(&mut account_trie).await;

            (incremental_root, incremental_nodes)
        };

        let (fused_root, fused_nodes) = {
            // ------------------------------------------------------------------------------
            // Take B: generate intermediate hashes for the accounts as of Block 1 in one go,
            // without increment_intermediate_hashes
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
            let mut account_trie = txn.mutable_cursor(tables::TrieAccount).await.unwrap();

            // Accounts [0,N) now hold 2 ETH
            for i in 0..N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, two_eth).await.unwrap();
            }

            // Accounts [N,2N) are deleted

            // Accounts [2N,4N) hold 1 ETH
            for i in 2 * N..4 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).await.unwrap();
            }

            let fused_root = regenerate_intermediate_hashes(&txn, &temp_dir, None)
                .await
                .unwrap();

            let fused_nodes = read_all_nodes(&mut account_trie).await;

            (fused_root, fused_nodes)
        };

        // ------------------------------------------------------------------------------
        // A and B should yield the same result
        // ------------------------------------------------------------------------------
        assert_eq!(fused_root, incremental_root);
        assert_eq!(fused_nodes, incremental_nodes);
    }

    #[tokio::test]
    async fn incremental_vs_regeneration_for_storage() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_database().unwrap();

        const N: u128 = 2_000;

        let account1 = Account {
            nonce: 5,
            balance: 7.as_u256() * ETHER,
            code_hash: hex!("5e3c5ae99a1c6785210d0d233641562557ad763e18907cca3a8d42bd0a0b4ecb")
                .into(),
        };

        let account2 = Account {
            nonce: 1,
            balance: 13.as_u256() * ETHER,
            code_hash: hex!("3a9c1d84e48734ae951e023197bda6d03933a4ca44124a2a544e227aa93efe75")
                .into(),
        };

        let address1 = H160(hex!("1000000000000000000000000000000000000000"));
        let address2 = H160(hex!("2000000000000000000000000000000000000000"));

        async fn upsert_storage_for_two_test_accounts<
            'tx,
            HS: MutableCursorDupSort<'tx, tables::HashedStorage>,
            SCT: MutableCursorDupSort<'tx, tables::StorageChangeSet>,
        >(
            hashed_storage: &mut HS,
            storage_change_table: &mut SCT,
            address1: Address,
            address2: Address,
            i: u128,
            value: U256,
            register_change: bool,
        ) {
            let plain_loc1 = u256_to_h256((2 * i).as_u256());
            let plain_loc2 = u256_to_h256((2 * i + 1).as_u256());

            upsert_hashed_storage_value(
                hashed_storage,
                keccak256(address1),
                keccak256(plain_loc1),
                value,
            )
            .await
            .unwrap();
            upsert_hashed_storage_value(
                hashed_storage,
                keccak256(address2),
                keccak256(plain_loc2),
                value,
            )
            .await
            .unwrap();
            if register_change {
                storage_change_table
                    .upsert(
                        tables::StorageChangeKey {
                            block_number: BlockNumber(1),
                            address: address1,
                        },
                        tables::StorageChange {
                            location: plain_loc1,
                            value: U256::ZERO,
                        },
                    )
                    .await
                    .unwrap();
                storage_change_table
                    .upsert(
                        tables::StorageChangeKey {
                            block_number: BlockNumber(1),
                            address: address2,
                        },
                        tables::StorageChange {
                            location: plain_loc2,
                            value: U256::ZERO,
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        let value_x = 0x42.as_u256();
        let value_y = U256::from_be_bytes(hex!(
            "71f602b294119bf452f1923814f5c6de768221254d3056b1bd63e72dc3142a29"
        ));
        {
            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();

            hashed_accounts
                .upsert(keccak256(address1), account1)
                .await
                .unwrap();
            hashed_accounts
                .upsert(keccak256(address2), account2)
                .await
                .unwrap();

            drop(hashed_accounts);
            txn.commit().await.unwrap();
        }

        let (incremental_root, incremental_nodes) = {
            // ------------------------------------------------------------------------------
            // Take A: create some storage at genesis and then apply some changes at Block 1
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().await.unwrap();

            let mut hashed_storage = txn
                .mutable_cursor_dupsort(tables::HashedStorage)
                .await
                .unwrap();
            let mut storage_change_table = txn
                .mutable_cursor_dupsort(tables::StorageChangeSet)
                .await
                .unwrap();
            let mut storage_trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();

            // Start with 3n storage slots per account at genesis, each with the same value
            for i in 0..3 * N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    value_x,
                    false,
                )
                .await;
            }

            regenerate_intermediate_hashes(&txn, &temp_dir, None)
                .await
                .unwrap();

            // Change the value of the first third of the storage
            for i in 0..N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    value_y,
                    true,
                )
                .await;
            }

            // Delete the second third of the storage
            for i in N..2 * N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    U256::ZERO,
                    true,
                )
                .await;
            }

            // Don't touch the last third of genesis storage

            // And add some new storage
            for i in 3 * N..4 * N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    value_x,
                    true,
                )
                .await;
            }

            let incremental_root =
                increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None)
                    .await
                    .unwrap();

            let incremental_nodes = read_all_nodes(&mut storage_trie).await;

            (incremental_root, incremental_nodes)
        };

        let (fused_root, fused_nodes) = {
            // ------------------------------------------------------------------------------
            // Take B: generate intermediate hashes for the storage as of Block 1 in one go,
            // without increment_intermediate_hashes
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_storage = txn
                .mutable_cursor_dupsort(tables::HashedStorage)
                .await
                .unwrap();
            let mut storage_change_table = txn
                .mutable_cursor_dupsort(tables::StorageChangeSet)
                .await
                .unwrap();
            let mut storage_trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();

            // The first third of the storage now has value_y
            for i in 0..N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    value_y,
                    false,
                )
                .await;
            }

            // The second third of the storage is deleted

            // The last third and the extra storage has value_x
            for i in 2 * N..4 * N {
                upsert_storage_for_two_test_accounts(
                    &mut hashed_storage,
                    &mut storage_change_table,
                    address1,
                    address2,
                    i,
                    value_x,
                    false,
                )
                .await;
            }

            let fused_root = regenerate_intermediate_hashes(&txn, &temp_dir, None)
                .await
                .unwrap();

            let fused_nodes = read_all_nodes(&mut storage_trie).await;

            (fused_root, fused_nodes)
        };

        // ------------------------------------------------------------------------------
        // A and B should yield the same result
        // ------------------------------------------------------------------------------
        assert_eq!(fused_root, incremental_root);
        assert_eq!(fused_nodes, incremental_nodes);
    }

    #[tokio::test]
    async fn storage_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address = hex!("1000000000000000000000000000000000000000").into();
        let hashed_address = keccak256(address);

        let account = Account {
            nonce: 1,
            balance: 15.as_u256() * ETHER,
            code_hash: hex!("7792ad513ce4d8f49163e21b25bf27ce6c8a0fa1e78c564e7d20a2d303303ba0")
                .into(),
        };

        let mut hashed_accounts = txn.mutable_cursor(tables::HashedAccount).await.unwrap();
        let mut hashed_storage = txn
            .mutable_cursor_dupsort(tables::HashedStorage)
            .await
            .unwrap();
        let mut storage_trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();

        hashed_accounts
            .upsert(hashed_address, account)
            .await
            .unwrap();

        let plain_location1 =
            hex!("1000000000000000000000000000000000000000000000000000000000000000").into();
        let plain_location2 =
            hex!("1A00000000000000000000000000000000000000000000000000000000000000").into();
        let plain_location3 =
            hex!("1E00000000000000000000000000000000000000000000000000000000000000").into();

        let hashed_location1 = keccak256(plain_location1);
        let hashed_location2 = keccak256(plain_location2);
        let hashed_location3 = keccak256(plain_location3);

        let value1 = 0xABCD.as_u256();
        let value2 = 0x4321.as_u256();
        let value3 = 0x4444.as_u256();

        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location1,
            value1,
        )
        .await
        .unwrap();
        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location2,
            value2,
        )
        .await
        .unwrap();
        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location3,
            value3,
        )
        .await
        .unwrap();

        regenerate_intermediate_hashes(&txn, &temp_dir, None)
            .await
            .unwrap();

        // There should be one root node in storage trie
        let nodes_a = read_all_nodes(&mut storage_trie).await;
        assert_eq!(nodes_a.len(), 1);

        drop(hashed_accounts);
        drop(hashed_storage);
        drop(storage_trie);
        txn.commit().await.unwrap();

        {
            // Increment the trie without any changes
            let txn = db.begin_mutable().await.unwrap();
            let mut storage_trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();
            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None)
                .await
                .unwrap();
            let nodes_b = read_all_nodes(&mut storage_trie).await;
            assert_eq!(nodes_b, nodes_a);
        }

        {
            // Delete storage and increment the trie
            let txn = db.begin_mutable().await.unwrap();
            let mut hashed_storage = txn
                .mutable_cursor_dupsort(tables::HashedStorage)
                .await
                .unwrap();
            let mut storage_change_table = txn
                .mutable_cursor_dupsort(tables::StorageChangeSet)
                .await
                .unwrap();
            let mut storage_trie = txn.mutable_cursor(tables::TrieStorage).await.unwrap();

            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location1,
                U256::ZERO,
            )
            .await
            .unwrap();
            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location2,
                U256::ZERO,
            )
            .await
            .unwrap();
            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location3,
                U256::ZERO,
            )
            .await
            .unwrap();

            let storage_change_key = tables::StorageChangeKey {
                block_number: BlockNumber(1),
                address,
            };

            storage_change_table
                .upsert(
                    storage_change_key,
                    tables::StorageChange {
                        location: plain_location1,
                        value: U256::ZERO,
                    },
                )
                .await
                .unwrap();
            storage_change_table
                .upsert(
                    storage_change_key,
                    tables::StorageChange {
                        location: plain_location2,
                        value: U256::ZERO,
                    },
                )
                .await
                .unwrap();
            storage_change_table
                .upsert(
                    storage_change_key,
                    tables::StorageChange {
                        location: plain_location3,
                        value: U256::ZERO,
                    },
                )
                .await
                .unwrap();

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None)
                .await
                .unwrap();
            let nodes_b = read_all_nodes(&mut storage_trie).await;
            assert!(nodes_b.is_empty());
        }
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
