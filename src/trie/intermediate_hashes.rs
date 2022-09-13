use crate::{
    consensus::{DuoError, ValidationError},
    crypto::keccak256,
    etl::collector::{TableCollector, OPTIMAL_BUFFER_CAPACITY},
    kv::{mdbx::*, tables, traits::*},
    models::*,
    stagedsync::format_duration,
    trie::{
        hash_builder::{pack_nibbles, unpack_nibbles, HashBuilder},
        node::Node,
        prefix_set::PrefixSet,
        util::has_prefix,
    },
};
use anyhow::{bail, Result};
use bytes::{BufMut, BytesMut};
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::{
    cmp,
    marker::PhantomData,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tracing::info;

fn key_is_before(k1: &[u8], k2: &[u8]) -> bool {
    k1 < k2
}

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
        if let Some(node) = &self.node {
            if self.nibble >= 0 {
                return node.state_mask & (1u16 << self.nibble) != 0;
            }
        }
        true
    }

    fn tree_flag(&self) -> bool {
        if let Some(node) = &self.node {
            if self.nibble >= 0 {
                return node.tree_mask & (1u16 << self.nibble) != 0;
            }
        }
        true
    }

    fn hash_flag(&self) -> bool {
        match &self.node {
            Some(node) => match self.nibble {
                -1 => node.root_hash.is_some(),
                _ => node.hash_mask & (1u16 << self.nibble) != 0,
            },
            None => false,
        }
    }

    fn hash(&self) -> Option<H256> {
        if self.hash_flag() {
            let node = self.node.as_ref().unwrap();
            match self.nibble {
                -1 => node.root_hash,
                _ => Some(node.hash_for_nibble(self.nibble)),
            }
        } else {
            None
        }
    }
}

struct SubNode {
    node: Node,
    key: BytesMut,
    value: BytesMut,
    child_id: i8,
    max_child_id: i8,
    hash_id: i8,
    deleted: bool,
}

impl SubNode {
    fn new() -> Self {
        Self {
            node: Default::default(),
            key: BytesMut::new(),
            value: BytesMut::new(),
            child_id: 0,
            max_child_id: 0,
            hash_id: 0,
            deleted: false,
        }
    }

    fn has_tree(&self) -> bool {
        self.node.tree_mask & (1 << self.child_id) != 0
    }

    fn has_hash(&self) -> bool {
        self.node.hash_mask & (1 << self.child_id) != 0
    }

    fn has_state(&self) -> bool {
        self.node.state_mask & (1 << self.child_id) != 0
    }

    fn reset(&mut self) {
        self.key.clear();
        self.value.clear();
        self.node.reset();
        self.child_id = -1;
        self.max_child_id = 0x10;
        self.hash_id = -1;
        self.deleted = false;
    }

    fn parse(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.key = BytesMut::from(k);
        self.value = BytesMut::from(v);

        self.node = Node::decode_from_storage(v)?;

        self.child_id = self.node.state_mask.trailing_zeros() as i8 - 1;
        self.max_child_id = 16 - self.node.state_mask.leading_zeros() as i8;
        self.hash_id = -1;
        self.deleted = false;

        Ok(())
    }

    fn full_key(&self) -> BytesMut {
        let mut key = self.key.clone();
        if self.child_id != -1 {
            key.put_i8(self.child_id);
        }
        key
    }

    fn hash(&self) -> Result<H256> {
        if self.hash_id < 0 || self.hash_id > 0xf {
            bail!("Hash id is out of bounds");
        }
        Ok(self.node.hashes[self.hash_id as usize])
    }
}

fn increment_nibbled_key(unpacked: &[u8]) -> Option<Vec<u8>> {
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

fn prefix_to_h256(slice: &[u8]) -> H256 {
    let mut buffer = slice.to_vec();
    buffer.resize(32, 0u8);
    H256::from_slice(&*buffer)
}

struct MoveOperation {
    key: Option<Vec<u8>>,
    hash: Option<H256>,
    children_in_trie: bool,
    first_uncovered: Option<Vec<u8>>,
}

pub struct Cursor<'cu, 'tx, 'ps, T>
where
    T: Table<Key = Vec<u8>, SeekKey = Vec<u8>, Value = Vec<u8>>,
    'tx: 'cu,
{
    cursor: Mutex<&'cu mut MdbxCursor<'tx, RW, T>>,
    changed: &'ps mut Option<PrefixSet>,
    prefix: Vec<u8>,
    stack: Vec<CursorSubNode>,
    sub_nodes: Vec<SubNode>,
    can_skip_state: bool,
    level: usize,
    end_of_tree: bool,
    curr_key: BytesMut,
    prev_key: BytesMut,
    next_created: BytesMut,
    collector: Option<Vec<Vec<u8>>>,
    _marker: PhantomData<&'tx T>,
}

impl<'cu, 'tx, 'ps, T> Cursor<'cu, 'tx, 'ps, T>
where
    T: Table<Key = Vec<u8>, SeekKey = Vec<u8>, Value = Vec<u8>>,
    'tx: 'cu,
{
    fn new(
        cursor: &'cu mut MdbxCursor<'tx, RW, T>,
        changed: &'ps mut Option<PrefixSet>,
        prefix: &[u8],
    ) -> Result<Cursor<'cu, 'tx, 'ps, T>> {
        let mut new_cursor = Self {
            cursor: Mutex::new(cursor),
            changed,
            prefix: prefix.to_vec(),
            stack: vec![],
            sub_nodes: vec![],
            can_skip_state: false,
            level: 0,
            end_of_tree: false,
            curr_key: BytesMut::new(),
            prev_key: BytesMut::new(),
            next_created: BytesMut::new(),
            collector: None,
            _marker: PhantomData,
        };
        new_cursor.consume_node(&[], true)?;
        Ok(new_cursor)
    }

    fn advance_to_prefix(&mut self, prefix: Vec<u8>) -> Result<MoveOperation> {
        if !prefix.is_empty() && prefix.len() != 40 {
            bail!("Invalid prefix len: expected 0 or 40, got {}", prefix.len());
        }
        self.prefix = prefix;

        let mut has_changes = self.changed.is_none();
        if !has_changes {
            let next_created;
            (has_changes, next_created) = self
                .changed
                .as_mut()
                .unwrap()
                .contains_and_next_sorted(self.prefix.as_slice(), self.prefix.len());
            self.next_created = BytesMut::from(next_created.as_slice());
        }

        let root_node_in_db = self.db_seek(&[])?;
        if root_node_in_db {
            let hash = self.sub_node().node.root_hash;

            if hash.is_none() {
                bail!(
                    "Trie integrity failure. Requested root node with key 0x{} has no root hash",
                    hex::encode(self.sub_node().full_key()),
                );
            }

            if !has_changes {
                self.end_of_tree = true;
                return Ok(MoveOperation {
                    key: Some(self.curr_key.to_vec()),
                    hash,
                    children_in_trie: false,
                    first_uncovered: None,
                });
            }

            self.db_delete_sub_node();
        } else {
            self.can_skip_state = false;
            self.end_of_tree = true;

            return Ok(MoveOperation {
                key: None,
                hash: None,
                children_in_trie: false,
                first_uncovered: None,
            });
        }

        self.advance_to_next()
    }

    #[inline(always)]
    fn sub_node(&self) -> &SubNode {
        &self.sub_nodes[self.level]
    }

    #[inline(always)]
    fn sub_node_mut(&mut self) -> &mut SubNode {
        &mut self.sub_nodes[self.level]
    }

    fn advance_to_next(&mut self) -> Result<MoveOperation> {
        if self.end_of_tree {
            bail!("Can't move next beyond the end of tree");
        }

        self.can_skip_state = true;
        self.prev_key = self.curr_key.clone();
        self.curr_key.clear();

        while !self.end_of_tree {
            self.sub_node_mut().child_id += 1;

            if self.sub_node().child_id == self.sub_node().max_child_id {
                if self.level > 0 {
                    self.level -= 1;
                } else {
                    self.end_of_tree = true;
                }
                continue;
            }

            if self.consume_sub_node() {
                self.curr_key.put(self.sub_node().full_key());
                return Ok(MoveOperation {
                    key: Some(self.curr_key.as_ref().into()),
                    hash: Some(self.sub_node().hash()?),
                    children_in_trie: self.sub_node().has_tree(),
                    first_uncovered: self.first_uncovered(),
                });
            }

            if self.sub_node().has_tree() {
                let key = self.sub_node().full_key();
                if !self.db_seek(key.as_ref())? {
                    bail!(
                        "Trie integrity failure. Missing child for node key=0x{}, child_id={}",
                        hex::encode(self.sub_node().key.as_ref()),
                        self.sub_node().child_id,
                    );
                } else {
                    self.can_skip_state = false;
                }
            }

            if self.sub_node().has_state() {
                self.can_skip_state = false;
            }
        }

        let next = increment_nibbled_key(self.prev_key.as_ref());
        self.can_skip_state &= next.is_none();

        Ok(MoveOperation {
            key: None,
            hash: None,
            children_in_trie: false,
            first_uncovered: self.first_uncovered(),
        })
    }

    fn next(&mut self) -> Result<()> {
        if let Some(last) = self.stack.last() {
            if !self.can_skip_state && self.children_are_in_trie() {
                match last.nibble {
                    -1 => self.move_to_next_sibling(true)?,
                    _ => self.consume_node(&self.key().unwrap(), false)?,
                }
            } else {
                self.move_to_next_sibling(false)?;
            }
            self.update_skip_state();
        }

        Ok(())
    }

    fn key(&self) -> Option<Vec<u8>> {
        self.stack.last().map(|n| n.full_key())
    }

    fn hash(&self) -> Option<H256> {
        self.stack.last().and_then(|n| n.hash())
    }

    fn children_are_in_trie(&self) -> bool {
        self.stack.last().map_or(false, |n| n.tree_flag())
    }

    fn first_uncovered_prefix(&self) -> Option<Vec<u8>> {
        match &self.key() {
            Some(key) => {
                if self.can_skip_state {
                    increment_nibbled_key(key).map(|k| pack_nibbles(k.as_slice()))
                } else {
                    Some(pack_nibbles(key.as_slice()))
                }
            }
            None => None,
        }
    }

    fn consume_sub_node(&mut self) -> bool {
        if self.sub_node().has_hash() {
            let mut buffer = self.prefix.clone();
            buffer.extend_from_slice(self.sub_node().full_key().as_ref());
            let (has_changes, next_created) = self
                .changed
                .as_mut()
                .unwrap()
                .contains_and_next_sorted(buffer.as_ref(), self.prefix.len());
            if !has_changes {
                self.can_skip_state &= key_is_before(buffer.as_slice(), &self.next_created);
                self.next_created.clear();
                self.next_created.put(next_created.as_slice());
                return true;
            }
        }

        self.db_delete_sub_node();

        false
    }

    fn first_uncovered(&mut self) -> Option<Vec<u8>> {
        if self.can_skip_state {
            None
        } else if self.prev_key.is_empty() {
            // Don't mark empty origin as overflown
            Some(vec![])
        } else {
            increment_nibbled_key(self.prev_key.as_mut()).map(|n| pack_nibbles(&n))
        }
    }

    fn db_delete_sub_node(&mut self) {
        if self.collector.is_some() && !self.sub_node().deleted {
            let mut buffer = self.prefix.clone();
            buffer.extend_from_slice(self.sub_node().key.as_ref());
            self.collector.as_mut().unwrap().push(buffer);
            self.sub_node_mut().deleted = true;
        }
    }

    fn db_seek(&mut self, seek_key: &[u8]) -> Result<bool> {
        let mut buffer = self.prefix.clone();
        buffer.extend_from_slice(seek_key);
        let data = if buffer.is_empty() {
            self.cursor.lock().first()
        } else {
            self.cursor.lock().seek_exact(buffer)
        }?;

        if let Some((ref key, ref value)) = data {
            if !seek_key.is_empty() {
                self.level += 1;
            }
            self.sub_node_mut().parse(key, value)?;
        }

        Ok(data.is_some())
    }

    fn consume_node(&mut self, to: &[u8], exact: bool) -> Result<()> {
        let db_key = [self.prefix.as_slice(), to].concat().to_vec();
        let entry = if exact {
            self.cursor.lock().seek_exact(db_key)?
        } else {
            self.cursor.lock().seek(db_key)?
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

        let node = if let Some((_, ref v)) = entry {
            let n = Node::decode_from_storage(v)?;
            if n.state_mask == 0 {
                bail!("Node with empty state mask");
            }
            Some(n)
        } else {
            None
        };

        let nibble = match &node {
            Some(n) if n.root_hash.is_none() => {
                (0i8..16).find(|i| n.state_mask & (1u16 << i) != 0).unwrap()
            }
            _ => -1,
        };

        if !key.is_empty() && !self.stack.is_empty() {
            self.stack[0].nibble = key[0] as i8;
        }
        self.stack.push(CursorSubNode { key, node, nibble });

        self.update_skip_state();

        if entry.is_some() && (!self.can_skip_state || nibble != -1) {
            self.cursor.lock().delete_current()?;
        }

        Ok(())
    }

    fn move_to_next_sibling(
        &mut self,
        allow_root_to_child_nibble_within_subnode: bool,
    ) -> Result<()> {
        if let Some(sn) = self.stack.last_mut() {
            if sn.nibble >= 15 || (sn.nibble < 0 && !allow_root_to_child_nibble_within_subnode) {
                self.stack.pop();
                self.move_to_next_sibling(false)?;
                return Ok(());
            }

            sn.nibble += 1;

            if sn.node.is_none() {
                let key = self.key();
                self.consume_node(key.as_ref().unwrap(), false)?;
                return Ok(());
            }

            while sn.nibble < 16 {
                if sn.state_flag() {
                    return Ok(());
                }
                sn.nibble += 1;
            }

            self.stack.pop();
            self.move_to_next_sibling(false)?;
        }
        Ok(())
    }

    fn update_skip_state(&mut self) {
        self.can_skip_state = if let Some(key) = self.key() {
            let s = [self.prefix.as_slice(), key.as_slice()].concat();
            !self.changed.as_mut().unwrap().contains(s.as_slice())
                && self.stack.last().unwrap().hash_flag()
        } else {
            false
        }
    }
}

pub struct TrieLoader2<'db, 'tx, 'tmp, 'co, E>
where
    E: EnvironmentKind,
    'db: 'tx,
    'tmp: 'co,
{
    txn: &'tx MdbxTransaction<'db, RW, E>,
    account_changes: Option<PrefixSet>,
    storage_changes: Option<PrefixSet>,
    account_trie_node_collector: &'co mut TableCollector<'tmp, tables::TrieAccount>,
    storage_trie_node_collector: &'co mut TableCollector<'tmp, tables::TrieStorage>,
}

impl<'db, 'tx, 'tmp, 'co, E> TrieLoader2<'db, 'tx, 'tmp, 'co, E>
where
    E: EnvironmentKind,
    'db: 'tx,
    'tmp: 'co,
{
    fn new(
        txn: &'tx MdbxTransaction<'db, RW, E>,
        account_changes: Option<PrefixSet>,
        storage_changes: Option<PrefixSet>,
        account_trie_node_collector: &'co mut TableCollector<'tmp, tables::TrieAccount>,
        storage_trie_node_collector: &'co mut TableCollector<'tmp, tables::TrieStorage>,
    ) -> Result<Self> {
        if account_changes.is_none() != storage_changes.is_none() {
            bail!("TrieLoader requires account and storage changes to be both provided, or both none.")
        }
        Ok(Self {
            txn,
            account_changes,
            storage_changes,
            account_trie_node_collector,
            storage_trie_node_collector,
        })
    }

    pub fn calculate_root(&mut self) -> Result<H256> {
        let mut hashed_accounts = self.txn.cursor(tables::HashedAccount)?;
        let mut hashed_storage = self.txn.cursor(tables::HashedStorage)?;
        let mut trie_accounts = self.txn.cursor(tables::TrieAccount)?;
        let mut trie_storage = self.txn.cursor(tables::TrieStorage)?;

        if self.account_changes.is_none()
            && (trie_accounts.next()?.is_some() || trie_storage.next()?.is_some())
        {
            bail!("Full regeneration detected but trie tables aren't empty.");
        }

        let storage_prefix_buffer = vec![];

        let account_node_collector = |nibbled_key: &[u8], node: &Node| {
            let value = if node.state_mask != 0 {
                node.encode_for_storage()
            } else {
                vec![]
            };
            self.account_trie_node_collector
                .push(nibbled_key.to_vec(), value);
        };

        let storage_node_collector = |nibbled_key: &[u8], node: &Node| {
            let mut full_key = storage_prefix_buffer.clone();
            full_key.extend_from_slice(nibbled_key);
            let value = if node.state_mask != 0 {
                node.encode_for_storage()
            } else {
                vec![]
            };
            self.storage_trie_node_collector.push(full_key, value);
        };

        let mut account_hash_builder = HashBuilder::new(Some(Box::new(account_node_collector)));
        let mut storage_hash_builder = HashBuilder::new(Some(Box::new(storage_node_collector)));

        let mut trie_account_cursor =
            Cursor::new(&mut trie_accounts, &mut self.account_changes, &[])?;
        let mut trie_storage_cursor =
            Cursor::new(&mut trie_storage, &mut self.storage_changes, &[])?;

        let mut trie_account_data = trie_account_cursor.advance_to_prefix(vec![])?;
        loop {
            if let Some(ref first_uncovered) = trie_account_data.first_uncovered {
                let mut hashed_account_data = if first_uncovered.is_empty() {
                    hashed_accounts.first()
                } else {
                    let seek_key = prefix_to_h256(first_uncovered);
                    hashed_accounts.seek(seek_key)
                }?;

                while let Some((address_hash, ref account)) = hashed_account_data {
                    let hashed_account_data_key_nibbled = unpack_nibbles(address_hash.as_bytes());

                    if let Some(ref key) = trie_account_data.key {
                        if *key < hashed_account_data_key_nibbled {
                            break;
                        }
                    }

                    let storage_root = caclulate_storage_root(
                        &mut trie_storage_cursor,
                        &mut storage_hash_builder,
                        &mut hashed_storage,
                        address_hash,
                    )?;

                    account_hash_builder.add_leaf(
                        hashed_account_data_key_nibbled,
                        &fastrlp::encode_fixed_size(&account.to_rlp(storage_root)),
                    );

                    hashed_account_data = hashed_accounts.next()?;
                }
            }

            if let Some(ref key) = trie_account_data.key {
                account_hash_builder.add_branch_node(
                    key.clone(),
                    &trie_account_data.hash.unwrap(),
                    trie_account_data.children_in_trie,
                );
                if key.is_empty() {
                    break;
                }
            } else {
                break;
            }

            trie_account_data = trie_account_cursor.advance_to_next()?;
        }

        Ok(account_hash_builder.compute_root_hash())
    }
}

pub fn caclulate_storage_root(
    trie_storage_cursor: &mut Cursor<tables::TrieStorage>,
    storage_hash_builder: &mut HashBuilder,
    hashed_storage: &mut MdbxCursor<RW, tables::HashedStorage>,
    db_storage_prefix: H256,
) -> Result<H256> {
    let trie_storage_data =
        trie_storage_cursor.advance_to_prefix(db_storage_prefix.as_ref().to_vec())?;
    loop {
        if let Some(ref first_uncovered) = trie_storage_data.first_uncovered {
            let seek_key = prefix_to_h256(first_uncovered);
            let mut hash_storage_data =
                hashed_storage.seek_both_range(db_storage_prefix, seek_key)?;
            while let Some((location_hash, value)) = hash_storage_data {
                let nibbled_location = unpack_nibbles(location_hash.as_ref());

                if let Some(ref key) = trie_storage_data.key {
                    if *key < nibbled_location {
                        break;
                    }
                }

                let encoded = fastrlp::encode_fixed_size(&value);
                storage_hash_builder.add_leaf(nibbled_location, encoded.as_slice());
                hash_storage_data = hashed_storage.next_dup()?.map(|(_, v)| v);
            }
        }

        if let Some(ref key) = trie_storage_data.key {
            storage_hash_builder.add_branch_node(
                key.clone(),
                &trie_storage_data.hash.unwrap(),
                trie_storage_data.children_in_trie,
            );
            if key.is_empty() {
                break;
            }
        } else {
            break;
        }
    }

    let storage_root = storage_hash_builder.compute_root_hash();
    storage_hash_builder.reset();

    Ok(storage_root)
}

pub struct TrieLoader<'db, 'tx, 'tmp, 'co, 'nc, E>
where
    E: EnvironmentKind,
    'db: 'tx,
    'tmp: 'co,
    'co: 'nc,
{
    txn: &'tx MdbxTransaction<'db, RW, E>,
    hb: HashBuilder<'nc>,
    storage_collector: &'co mut TableCollector<'tmp, tables::TrieStorage>,
    rlp: Vec<u8>,
    _marker: PhantomData<&'db ()>,
}

impl<'db, 'tx, 'tmp, 'co, 'nc, E> TrieLoader<'db, 'tx, 'tmp, 'co, 'nc, E>
where
    E: EnvironmentKind,
    'db: 'tx,
    'tmp: 'co,
    'co: 'nc,
{
    pub fn new(
        txn: &'tx MdbxTransaction<'db, RW, E>,
        account_collector: &'co mut TableCollector<'tmp, tables::TrieAccount>,
        storage_collector: &'co mut TableCollector<'tmp, tables::TrieStorage>,
    ) -> Self {
        let node_collector = |unpacked_key: &[u8], node: &Node| {
            if !unpacked_key.is_empty() {
                account_collector.push(unpacked_key.to_vec(), node.encode_for_storage());
            }
        };

        Self {
            txn,
            hb: HashBuilder::new(Some(Box::new(node_collector))),
            storage_collector,
            rlp: vec![],
            _marker: PhantomData,
        }
    }

    pub fn calculate_root(
        &mut self,
        account_changes: &mut PrefixSet,
        storage_changes: &mut PrefixSet,
    ) -> Result<H256> {
        let mut state = self.txn.cursor(tables::HashedAccount)?;
        let mut trie_db_cursor = self.txn.cursor(tables::TrieAccount)?;

        let mut changed = Some(account_changes.clone());
        let mut trie = Cursor::new(&mut trie_db_cursor, &mut changed, &[])?;

        let first_started_at = Instant::now();
        let mut last_message_at = Instant::now();

        while let Some(key) = trie.key() {
            if trie.can_skip_state {
                self.hb.add_branch_node(
                    key,
                    trie.hash().as_ref().unwrap(),
                    trie.children_are_in_trie(),
                );
            }

            let seek_key = match trie.first_uncovered_prefix() {
                Some(mut uncovered) => {
                    uncovered.resize(32, 0);
                    uncovered
                }
                None => break,
            };

            trie.next()?;

            let mut acc = state.seek(H256::from_slice(seek_key.as_slice()))?;
            let trie_key = trie.key();

            while let Some((address, account)) = acc {
                let packed_key = address.as_bytes();
                let unpacked_key = unpack_nibbles(packed_key);

                if let Some(ref key) = trie_key {
                    if key < &unpacked_key {
                        break;
                    }
                }

                let storage_root =
                    self.calculate_storage_root(address.as_bytes(), storage_changes)?;

                self.hb.add_leaf(
                    unpacked_key,
                    &fastrlp::encode_fixed_size(&account.to_rlp(storage_root)),
                );

                acc = state.next()?;

                let now = Instant::now();
                let elapsed = now - last_message_at;
                if elapsed >= Duration::from_secs(30) {
                    let total_elapsed = now - first_started_at;

                    let prefix = (packed_key[0] as u32) * 0x100 + packed_key[1] as u32;
                    let progress = prefix as f32 / 65536.0;

                    let total_time = total_elapsed.mul_f32(1.0 / progress);
                    let total_remaining = total_time.saturating_sub(total_elapsed);

                    info!(
                        "At prefix 0x{:04x}..., progress {:0>2.2}%. {} remaining",
                        prefix,
                        100.0 * progress,
                        format_duration(total_remaining, false),
                    );

                    last_message_at = Instant::now();
                }
            }
        }

        Ok(self.hb.compute_root_hash())
    }

    fn calculate_storage_root(
        &mut self,
        account_key: &[u8],
        changed: &mut PrefixSet,
    ) -> Result<H256> {
        let mut state = self.txn.cursor(tables::HashedStorage)?;

        let mut trie_db_cursor = self.txn.cursor(tables::TrieStorage)?;

        let mut hb = HashBuilder::new(Some(Box::new(
            |unpacked_storage_key: &[u8], node: &Node| {
                let key = [account_key, unpacked_storage_key].concat();
                self.storage_collector.push(key, node.encode_for_storage());
            },
        )));

        let mut changed = Some(changed.clone());
        let mut trie = Cursor::new(&mut trie_db_cursor, &mut changed, account_key)?;
        while let Some(key) = trie.key() {
            if trie.can_skip_state {
                if state.seek_exact(H256::from_slice(account_key))?.is_none() {
                    return Ok(EMPTY_ROOT);
                }
                hb.add_branch_node(
                    key,
                    trie.hash().as_ref().unwrap(),
                    trie.children_are_in_trie(),
                );
            }

            let seek_key = match trie.first_uncovered_prefix() {
                Some(mut uncovered) => {
                    uncovered.resize(32, 0);
                    uncovered
                }
                None => break,
            };

            trie.next()?;

            let mut storage = state.seek_both_range(
                H256::from_slice(account_key),
                H256::from_slice(seek_key.as_slice()),
            )?;
            let trie_key = trie.key();

            while let Some((storage_location, value)) = storage {
                let unpacked_loc = unpack_nibbles(storage_location.as_bytes());
                if let Some(ref key) = trie_key {
                    if key < &unpacked_loc {
                        break;
                    }
                }
                hb.add_leaf(unpacked_loc, fastrlp::encode_fixed_size(&value).as_ref());
                storage = state.next_dup()?.map(|(_, v)| v);
            }
        }

        Ok(hb.compute_root_hash())
    }
}

pub fn do_increment_intermediate_hashes<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    expected_root: Option<H256>,
    account_changes: &mut PrefixSet,
    storage_changes: &mut PrefixSet,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    let mut account_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);

    let root = {
        let mut loader = TrieLoader::new(txn, &mut account_collector, &mut storage_collector);
        loader.calculate_root(account_changes, storage_changes)?
    };

    if let Some(expected) = expected_root {
        if expected != root {
            return Err(DuoError::Validation(ValidationError::WrongStateRoot {
                expected,
                got: root,
            }));
        }
    }

    let mut target = txn.cursor(tables::TrieAccount.erased())?;
    account_collector.load(&mut target)?;

    let mut target = txn.cursor(tables::TrieStorage.erased())?;
    storage_collector.load(&mut target)?;

    Ok(root)
}

fn gather_account_changes<'db, 'tx, K, E>(
    txn: &'tx MdbxTransaction<'db, K, E>,
    from: BlockNumber,
) -> Result<PrefixSet>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let mut out = PrefixSet::new();

    let mut account_changes = txn.cursor(tables::AccountChangeSet)?;
    let mut data = account_changes.seek(from)?;
    while let Some((_, account_change)) = data {
        let hashed_address = keccak256(account_change.address);
        out.insert(unpack_nibbles(hashed_address.as_bytes()).as_slice());
        data = account_changes.next()?;
    }

    info!("Gathered {} account changes.", out.len());

    Ok(out)
}

fn collect_account_changes<'db, 'tx, K, E>(
    txn: &'tx MdbxTransaction<'db, K, E>,
    from: BlockNumber,
    to: BlockNumber,
    hashed_addresses: &mut BTreeMap<Address, H256>,
) -> Result<PrefixSet>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let forward = to > from;
    let mut expected_blocknum = BlockNumber(cmp::min(from.0, to.0) + 1);
    let max_blocknum = cmp::max(from, to);

    // let mut deleted_ts_prefixes = BTreeSet::new();
    let mut plainstate_accounts = LruCache::new(100_000usize);

    let mut ret = PrefixSet::new();

    let mut account_changeset = txn.cursor(tables::AccountChangeSet)?;
    let mut plain_state = txn.cursor(tables::Account)?;

    let mut changeset_data = account_changeset.seek(expected_blocknum)?;
    while let Some((reached_blocknum, _)) = changeset_data {
        if reached_blocknum > max_blocknum {
            break;
        }
        while let Some((_, ref change)) = changeset_data {
            let hashed_address = match hashed_addresses.get(&change.address) {
                Some(hash) => *hash,
                None => {
                    let hash = keccak256(&change.address);
                    hashed_addresses.insert(change.address, hash);
                    hash
                }
            };

            if !plainstate_accounts.contains(&change.address) {
                if let Some((_, account)) = plain_state.seek_exact(change.address)? {
                    plainstate_accounts.put(change.address, account);
                }
            }

            let plainstate_account = plainstate_accounts.get(&change.address);

            let mut account_created = false;

            if (forward && change.account.is_none()) || (!forward && plainstate_account.is_none()) {
                account_created = true;
            }
            // todo delete_ts_prefixes if not None

            ret.insert2(&unpack_nibbles(hashed_address.as_ref()), account_created);
            changeset_data = account_changeset.next_dup()?;
        }

        expected_blocknum += BlockNumber(expected_blocknum.0 + 1);
        changeset_data = account_changeset.next_no_dup()?;
    }

    Ok(ret)
}

fn gather_storage_changes<'db, 'tx, K, E>(
    txn: &'tx MdbxTransaction<'db, K, E>,
    account_changes: &mut PrefixSet,
    from: BlockNumber,
) -> Result<PrefixSet>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let mut out = PrefixSet::new();

    let mut storage_changes = txn.cursor(tables::StorageChangeSet)?;
    let mut data = storage_changes.seek(from)?;
    while let Some((key, storage_change)) = data {
        let hashed_address = keccak256(key.address);

        account_changes.insert(unpack_nibbles(hashed_address.as_bytes()).as_slice());

        let hashed_location = keccak256(storage_change.location);

        let hashed_key = [
            hashed_address.as_bytes(),
            unpack_nibbles(hashed_location.as_bytes()).as_slice(),
        ]
        .concat();

        out.insert(hashed_key.as_slice());
        data = storage_changes.next()?;
    }

    info!("Gathered {} storage changes.", out.len());

    Ok(out)
}

fn collect_storage_changes<'db, 'tx, K, E>(
    txn: &'tx MdbxTransaction<'db, K, E>,
    from: BlockNumber,
    to: BlockNumber,
    hashed_addresses: &mut BTreeMap<Address, H256>,
) -> Result<PrefixSet>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let expected_blocknum = BlockNumber(from.0 + 1);

    let mut ret = PrefixSet::new();

    let mut storage_changeset = txn.cursor(tables::StorageChangeSet)?;
    let mut changeset_data = storage_changeset.seek(expected_blocknum)?;

    while let Some((ref key, _)) = changeset_data {
        if key.block_number > to {
            break;
        }

        let hashed_address = match hashed_addresses.get(&key.address) {
            Some(hash) => *hash,
            None => {
                let hash = keccak256(&key.address);
                hashed_addresses.insert(key.address, hash);
                hash
            }
        };

        while let Some((_, ref value)) = changeset_data {
            let unpacked_hashed_location = unpack_nibbles(keccak256(value.location).as_ref());
            let mut prefix = Vec::with_capacity(ADDRESS_LENGTH + 2 * KECCAK_LENGTH);
            prefix.extend_from_slice(hashed_address.as_ref());
            prefix.extend_from_slice(unpacked_hashed_location.as_ref());

            let created = value.value == U256::ZERO;
            ret.insert2(prefix.as_slice(), created);

            changeset_data = storage_changeset.next_dup()?;
        }

        changeset_data = storage_changeset.next_no_dup()?;
    }

    Ok(ret)
}

pub fn increment_intermediate_hashes<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    from: BlockNumber,
    expected_root: Option<H256>,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    let mut account_changes = gather_account_changes(txn, from + 1)?;
    let mut storage_changes = gather_storage_changes(txn, &mut account_changes, from + 1)?;
    do_increment_intermediate_hashes(
        txn,
        etl_dir,
        expected_root,
        &mut account_changes,
        &mut storage_changes,
    )
}

pub fn increment_intermediate_hashes2<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    from: BlockNumber,
    to: BlockNumber,
    expected_root: Option<H256>,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    let mut account_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);

    let mut hashed_addresses = BTreeMap::new();
    let account_changes = collect_account_changes(txn, from, to, &mut hashed_addresses)?;
    let storage_changes = collect_storage_changes(txn, from, to, &mut hashed_addresses)?;
    drop(hashed_addresses);

    let mut trie_loader = TrieLoader2::new(
        txn,
        Some(account_changes),
        Some(storage_changes),
        &mut account_collector,
        &mut storage_collector,
    )?;

    let computed_root = trie_loader.calculate_root()?;

    if let Some(expected) = expected_root {
        if computed_root != expected {
            return Err(DuoError::Validation(ValidationError::WrongStateRoot {
                expected,
                got: computed_root,
            }));
        }
    }

    let mut target = txn.cursor(tables::TrieAccount.erased())?;
    account_collector.load(&mut target)?;

    let mut target = txn.cursor(tables::TrieStorage.erased())?;
    storage_collector.load(&mut target)?;

    Ok(computed_root)
}

pub fn unwind_intermediate_hashes<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    unwind_to: BlockNumber,
    expected_root: Option<H256>,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    let mut account_changes = gather_account_changes(txn, unwind_to)?;
    let mut storage_changes = gather_storage_changes(txn, &mut account_changes, unwind_to)?;
    do_increment_intermediate_hashes(
        txn,
        etl_dir,
        expected_root,
        &mut account_changes,
        &mut storage_changes,
    )
}

pub fn unwind_intermediate_hashes2<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    from: BlockNumber,
    to: BlockNumber,
) -> std::result::Result<(), DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    if to >= from {
        return Ok(());
    }

    let segment_width = from - to;

    let expected_root = None; // TODO fetch from db

    if segment_width > 100_000 {
        regenerate_intermediate_hashes2(txn, etl_dir, expected_root)?;
    } else {
        increment_intermediate_hashes2(txn, etl_dir, from, to, expected_root)?;
    }

    Ok(())
}

pub fn regenerate_intermediate_hashes<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    expected_root: Option<H256>,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    txn.clear_table(tables::TrieAccount)?;
    txn.clear_table(tables::TrieStorage)?;
    do_increment_intermediate_hashes(
        txn,
        etl_dir,
        expected_root,
        &mut PrefixSet::new(),
        &mut PrefixSet::new(),
    )
}

pub fn regenerate_intermediate_hashes2<'db, 'tx, E>(
    txn: &'tx MdbxTransaction<'db, RW, E>,
    etl_dir: &TempDir,
    expected_root: Option<H256>,
) -> std::result::Result<H256, DuoError>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    txn.clear_table(tables::TrieAccount)?;
    txn.clear_table(tables::TrieStorage)?;

    let mut account_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector = TableCollector::new(etl_dir, OPTIMAL_BUFFER_CAPACITY);

    let mut trie_loader = TrieLoader2::new(
        txn,
        None,
        None,
        &mut account_collector,
        &mut storage_collector,
    )?;

    let computed_root = trie_loader.calculate_root()?;

    if let Some(expected) = expected_root {
        if computed_root != expected {
            return Err(DuoError::Validation(ValidationError::WrongStateRoot {
                expected,
                got: computed_root,
            }));
        }
    }

    let mut target = txn.cursor(tables::TrieAccount.erased())?;
    account_collector.load(&mut target)?;

    let mut target = txn.cursor(tables::TrieStorage.erased())?;
    storage_collector.load(&mut target)?;

    Ok(computed_root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::{keccak256, trie_root},
        h256_to_u256,
        kv::{
            new_mem_chaindata, tables,
            tables::{AccountChange, StorageChange, StorageChangeKey},
        },
        trie::regenerate_intermediate_hashes,
        u256_to_h256, upsert_hashed_storage_value, zeroless_view,
    };
    use anyhow::Result;
    use bytes::BytesMut;
    use fastrlp::Encodable;
    use hex_literal::hex;
    use maplit::hashmap;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, HashMap};
    use tempfile::TempDir;

    #[test]
    fn test_intermediate_hashes_cursor_traversal_1() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();
        let mut trie = txn.cursor(tables::TrieAccount).unwrap();

        let key1 = vec![0x1u8];
        let node1 = Node::new(0b1011, 0b1001, 0, vec![], None);
        trie.upsert(key1, node1.encode_for_storage()).unwrap();

        let key2 = vec![0x1u8, 0x0, 0xB];
        let node2 = Node::new(0b1010, 0, 0, vec![], None);
        trie.upsert(key2, node2.encode_for_storage()).unwrap();

        let key3 = vec![0x1u8, 0x3];
        let node3 = Node::new(0b1110, 0, 0, vec![], None);
        trie.upsert(key3, node3.encode_for_storage()).unwrap();

        let mut changed = Some(PrefixSet::new());
        let mut cursor = Cursor::new(&mut trie, &mut changed, &[]).unwrap();

        assert!(cursor.key().unwrap().is_empty());

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0, 0xB, 0x1]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x0, 0xB, 0x3]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x1]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x1]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x2]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x1, 0x3, 0x3]);

        cursor.next().unwrap();
        assert!(cursor.key().is_none());
    }

    #[test]
    fn test_intermediate_hashes_cursor_traversal_2() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();
        let mut trie = txn.cursor(tables::TrieAccount).unwrap();

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
        trie.upsert(key1, node1.encode_for_storage()).unwrap();

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
        trie.upsert(key2, node2.encode_for_storage()).unwrap();

        let mut changed = Some(PrefixSet::new());
        let mut cursor = Cursor::new(&mut trie, &mut changed, &[]).unwrap();

        assert!(cursor.key().unwrap().is_empty());

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x4, 0x2]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x4, 0x4]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x6, 0x1]);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), vec![0x6, 0x4]);

        cursor.next().unwrap();
        assert!(cursor.key().is_none());
    }

    #[test]
    fn test_intermediate_hashes_cursor_traversal_within_prefix() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();
        let mut trie = txn.cursor(tables::TrieAccount).unwrap();

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
        trie.upsert(prefix_a, node_a.encode_for_storage()).unwrap();

        let node_b1 = Node::new(
            0b10100,
            0b00100,
            0,
            vec![],
            Some(H256::from(hex!(
                "c570b66136e99d07c6c6360769de1d9397805849879dd7c79cf0b8e6694bfb0e"
            ))),
        );
        trie.upsert(prefix_b.clone(), node_b1.encode_for_storage())
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
        trie.upsert(key_b2, node_b2.encode_for_storage()).unwrap();

        let node_c = Node::new(
            0b11110,
            0,
            0,
            vec![],
            Some(H256::from(hex!(
                "0f12bed8e3cc4cce692d234e69a4d79c0e74ab05ecb808dad588212eab788c31"
            ))),
        );
        trie.upsert(prefix_c.clone(), node_c.encode_for_storage())
            .unwrap();

        let mut changed = Some(PrefixSet::new());
        let mut cursor = Cursor::new(&mut trie, &mut changed, prefix_b.as_slice()).unwrap();

        assert!(cursor.key().unwrap().is_empty());
        assert!(cursor.can_skip_state);

        cursor.next().unwrap();
        assert!(cursor.key().is_none());

        let mut changed = PrefixSet::new();
        changed.insert([prefix_b.as_slice(), &[0xdu8, 0x5]].concat().as_slice());
        changed.insert([prefix_c.as_slice(), &[0xbu8, 0x8]].concat().as_slice());
        let mut changed = Some(changed);
        let mut cursor = Cursor::new(&mut trie, &mut changed, prefix_b.as_slice()).unwrap();

        assert!(cursor.key().unwrap().is_empty());
        assert!(!cursor.can_skip_state);

        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), [0x2]);
        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), [0x2, 0x1]);
        cursor.next().unwrap();
        assert_eq!(cursor.key().unwrap(), [0x4]);

        cursor.next().unwrap();
        assert!(cursor.key().is_none());
    }

    #[test]
    fn cursor_traversal_within_prefix() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

        let mut trie = txn.cursor(tables::TrieStorage).unwrap();

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
        trie.upsert(prefix_a.to_vec(), node_a.encode_for_storage())
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
        trie.upsert(prefix_b.to_vec(), node_b1.encode_for_storage())
            .unwrap();
        trie.upsert(
            [&prefix_b as &[u8], &([0x2] as [u8; 1]) as &[u8]].concat(),
            node_b2.encode_for_storage(),
        )
        .unwrap();

        let node_c = Node::new(
            0b11110,
            0,
            0,
            vec![],
            Some(hex!("0f12bed8e3cc4cce692d234e69a4d79c0e74ab05ecb808dad588212eab788c31").into()),
        );
        trie.upsert(prefix_c.to_vec(), node_c.encode_for_storage())
            .unwrap();

        // No changes
        let mut changed = Some(PrefixSet::new());
        let mut cursor = Cursor::new(&mut trie, &mut changed, &prefix_b).unwrap();

        assert_eq!(cursor.key(), Some(vec![])); // root
        assert!(cursor.can_skip_state); // due to root_hash
        cursor.next().unwrap(); // skips to end of trie
        assert_eq!(cursor.key(), None);

        // Some changes
        let mut changed = PrefixSet::new();
        changed.insert(&[&prefix_b as &[u8], &([0xD, 0x5] as [u8; 2]) as &[u8]].concat());
        changed.insert(&[&prefix_c as &[u8], &([0xB, 0x8] as [u8; 2]) as &[u8]].concat());
        let mut changed = Some(changed);
        let mut cursor = Cursor::new(&mut trie, &mut changed, &prefix_b).unwrap();

        assert_eq!(cursor.key(), Some(vec![])); // root
        assert!(!cursor.can_skip_state);
        cursor.next().unwrap();
        assert_eq!(cursor.key(), Some(vec![0x2]));
        cursor.next().unwrap();
        assert_eq!(cursor.key(), Some(vec![0x2, 0x1]));
        cursor.next().unwrap();
        assert_eq!(cursor.key(), Some(vec![0x4]));

        cursor.next().unwrap();
        assert_eq!(cursor.key(), None); // end of trie
    }

    fn setup_storage<E>(tx: &MdbxTransaction<'_, RW, E>, storage_key: H256) -> H256
    where
        E: EnvironmentKind,
    {
        let mut hashed_storage = tx.cursor(tables::HashedStorage).unwrap();

        let mut hb = HashBuilder::new(None);

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

            upsert_hashed_storage_value(&mut hashed_storage, storage_key, loc, val).unwrap();

            hb.add_leaf(
                unpack_nibbles(loc.as_bytes()),
                &fastrlp::encode_fixed_size(&val),
            );
        }

        hb.compute_root_hash()
    }

    fn read_all_nodes<K, T>(cursor: MdbxCursor<'_, K, T>) -> HashMap<Vec<u8>, Node>
    where
        K: TransactionKind,
        T: Table<Key = Vec<u8>, Value = Vec<u8>>,
    {
        cursor
            .walk(None)
            .map(|res| {
                let (k, v) = res.unwrap();
                (k, Node::decode_from_storage(&v).unwrap())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .collect()
    }

    #[test]
    fn account_and_storage_trie() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

        let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
        let mut hb = HashBuilder::new(None);

        let key1 = hex!("B000000000000000000000000000000000000000000000000000000000000000").into();
        let a1 = Account {
            nonce: 0,
            balance: 3.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key1, a1).unwrap();
        hb.add_leaf(
            unpack_nibbles(&key1[..]),
            &fastrlp::encode_fixed_size(&a1.to_rlp(EMPTY_ROOT)),
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
        hashed_accounts.upsert(key2, a2).unwrap();
        hb.add_leaf(
            unpack_nibbles(&key2[..]),
            &fastrlp::encode_fixed_size(&a2.to_rlp(EMPTY_ROOT)),
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
        hashed_accounts.upsert(key3, a3).unwrap();

        let storage_root = setup_storage(&txn, key3);

        hb.add_leaf(
            unpack_nibbles(&key3[..]),
            &fastrlp::encode_fixed_size(&a3.to_rlp(storage_root)),
        );

        let key4a = hex!("B1A0000000000000000000000000000000000000000000000000000000000000").into();
        let a4a = Account {
            nonce: 0,
            balance: 4.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key4a, a4a).unwrap();
        hb.add_leaf(
            unpack_nibbles(&key4a[..]),
            &fastrlp::encode_fixed_size(&a4a.to_rlp(EMPTY_ROOT)),
        );

        let key5 = hex!("B310000000000000000000000000000000000000000000000000000000000000").into();
        let a5 = Account {
            nonce: 0,
            balance: 8.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key5, a5).unwrap();
        hb.add_leaf(
            unpack_nibbles(&key5[..]),
            &fastrlp::encode_fixed_size(&a5.to_rlp(EMPTY_ROOT)),
        );

        let key6 = hex!("B340000000000000000000000000000000000000000000000000000000000000").into();
        let a6 = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };
        hashed_accounts.upsert(key6, a6).unwrap();
        hb.add_leaf(
            unpack_nibbles(&key6[..]),
            &fastrlp::encode_fixed_size(&a6.to_rlp(EMPTY_ROOT)),
        );

        // ----------------------------------------------------------------
        // Populate account & storage trie DB tables
        // ----------------------------------------------------------------

        regenerate_intermediate_hashes(&txn, &temp_dir, Some(hb.compute_root_hash())).unwrap();

        // ----------------------------------------------------------------
        // Check account trie
        // ----------------------------------------------------------------

        let node_map = read_all_nodes(txn.cursor(tables::TrieAccount).unwrap());
        assert_eq!(node_map.len(), 2);

        let node1a = &node_map[&vec![0xB]];

        assert_eq!(node1a.state_mask, 0b1011);
        assert_eq!(node1a.tree_mask, 0b0001);
        assert_eq!(node1a.hash_mask, 0b1001);

        assert_eq!(node1a.root_hash, None);
        assert_eq!(node1a.hashes.len(), 2);

        let node2a = &node_map[&vec![0xB, 0x0]];

        assert_eq!(node2a.state_mask, 0b10001);
        assert_eq!(node2a.tree_mask, 0b00000);
        assert_eq!(node2a.hash_mask, 0b10000);

        assert_eq!(node2a.root_hash, None);
        assert_eq!(node2a.hashes.len(), 1);

        // ----------------------------------------------------------------
        // Check storage trie
        // ----------------------------------------------------------------

        let node_map = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());
        assert_eq!(node_map.len(), 1);

        let node3 = &node_map[&key3.0.to_vec()];

        assert_eq!(node3.state_mask, 0b1010);
        assert_eq!(node3.tree_mask, 0b0000);
        assert_eq!(node3.hash_mask, 0b0010);

        assert_eq!(node3.root_hash, Some(storage_root));
        assert_eq!(node3.hashes.len(), 1);

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
            .unwrap();

        let mut account_change_table = txn.cursor(tables::AccountChangeSet).unwrap();
        account_change_table
            .upsert(
                BlockNumber(1),
                tables::AccountChange {
                    address: address4b,
                    account: None,
                },
            )
            .unwrap();

        increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None).unwrap();

        let node_map = read_all_nodes(txn.cursor(tables::TrieAccount).unwrap());
        assert_eq!(node_map.len(), 2);

        let node1b = &node_map[&vec![0xB]];
        assert_eq!(node1b.state_mask, 0b1011);
        assert_eq!(node1b.tree_mask, 0b0001);
        assert_eq!(node1b.hash_mask, 0b1011);

        assert_eq!(node1b.root_hash, None);

        assert_eq!(node1b.hashes.len(), 3);
        assert_eq!(node1a.hashes[0], node1b.hashes[0]);
        assert_eq!(node1a.hashes[1], node1b.hashes[2]);

        let node2b = &node_map[&vec![0xB, 0x0]];
        assert_eq!(node2a, node2b);

        drop(hashed_accounts);
        drop(account_change_table);
        txn.commit().unwrap();

        // Delete an account
        {
            let txn = db.begin_mutable().unwrap();
            let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
            let account_trie = txn.cursor(tables::TrieAccount).unwrap();
            let mut account_change_table = txn.cursor(tables::AccountChangeSet).unwrap();
            {
                let account = hashed_accounts.seek_exact(key2).unwrap().unwrap().1;
                hashed_accounts.delete_current().unwrap();
                account_change_table
                    .upsert(
                        BlockNumber(2),
                        tables::AccountChange {
                            address: address2,
                            account: Some(account),
                        },
                    )
                    .unwrap();
            }

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(1), None).unwrap();

            let node_map = read_all_nodes(account_trie);
            assert_eq!(node_map.len(), 1);

            let node1c = &node_map[&vec![0xB]];
            assert_eq!(node1c.state_mask, 0b1011);
            assert_eq!(node1c.tree_mask, 0b0000);
            assert_eq!(node1c.hash_mask, 0b1011);

            assert_eq!(node1c.root_hash, None);

            assert_eq!(node1c.hashes.len(), 3);
            assert_ne!(node1b.hashes[0], node1c.hashes[0]);
            assert_eq!(node1b.hashes[1], node1c.hashes[1]);
            assert_eq!(node1b.hashes[2], node1c.hashes[2]);
        }

        // Delete several accounts
        {
            let txn = db.begin_mutable().unwrap();
            let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
            let account_trie = txn.cursor(tables::TrieAccount).unwrap();
            let mut account_change_table = txn.cursor(tables::AccountChangeSet).unwrap();
            for (key, address) in [(key2, address2), (key3, address3)] {
                let account = hashed_accounts.seek_exact(key).unwrap().unwrap().1;
                hashed_accounts.delete_current().unwrap();
                account_change_table
                    .upsert(
                        BlockNumber(2),
                        tables::AccountChange {
                            address,
                            account: Some(account),
                        },
                    )
                    .unwrap();
            }

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(1), None).unwrap();

            assert_eq!(
                read_all_nodes(account_trie),
                hashmap! {
                    vec![0xB] => Node::new(0b1011, 0b0000, 0b1010, vec![node1b.hashes[1], node1b.hashes[2]], None)
                }
            );
        }
    }

    #[test]
    fn account_trie_around_extension_node() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

        let a = Account {
            nonce: 0,
            balance: 1.as_u256() * ETHER,
            ..Default::default()
        };

        let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
        let mut hb = HashBuilder::new(None);

        for key in [
            hex!("30af561000000000000000000000000000000000000000000000000000000000"),
            hex!("30af569000000000000000000000000000000000000000000000000000000000"),
            hex!("30af650000000000000000000000000000000000000000000000000000000000"),
            hex!("30af6f0000000000000000000000000000000000000000000000000000000000"),
            hex!("30af8f0000000000000000000000000000000000000000000000000000000000"),
            hex!("3100000000000000000000000000000000000000000000000000000000000000"),
        ] {
            hashed_accounts.upsert(H256(key), a).unwrap();
            hb.add_leaf(
                unpack_nibbles(&key[..]),
                &fastrlp::encode_fixed_size(&a.to_rlp(EMPTY_ROOT)),
            );
        }

        let expected_root = hb.compute_root_hash();
        assert_eq!(
            regenerate_intermediate_hashes(&txn, &temp_dir, Some(expected_root)).unwrap(),
            expected_root
        );

        let node_map = read_all_nodes(txn.cursor(tables::TrieAccount).unwrap());
        assert_eq!(node_map.len(), 2);

        assert_eq!(
            node_map[&vec![0x3]],
            Node::new(0b11, 0b01, 0b00, vec![], None)
        );

        let node2 = &node_map[&vec![0x3, 0x0, 0xA, 0xF]];

        assert_eq!(node2.state_mask, 0b101100000);
        assert_eq!(node2.tree_mask, 0b000000000);
        assert_eq!(node2.hash_mask, 0b001000000);

        assert_eq!(node2.root_hash, None);
        assert_eq!(node2.hashes.len(), 1);
    }

    fn int_to_address(i: u128) -> Address {
        let mut address = Address::zero();
        address[4..].copy_from_slice(&i.to_be_bytes());
        address
    }

    #[test]
    fn incremental_vs_regeneration() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_chaindata().unwrap();

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

            let txn = db.begin_mutable().unwrap();
            let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
            let mut account_change_table = txn.cursor(tables::AccountChangeSet).unwrap();

            // Start with 3n accounts at genesis, each holding 1 ETH
            for i in 0..3 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).unwrap();
            }

            regenerate_intermediate_hashes(&txn, &temp_dir, None).unwrap();

            let block_key = BlockNumber(1);

            // Double the balance of the first third of the accounts
            for i in 0..N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, two_eth).unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: Some(one_eth),
                        },
                    )
                    .unwrap();
            }

            // Delete the second third of the accounts
            for i in N..2 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                let account = hashed_accounts.seek_exact(hash).unwrap().unwrap().1;
                hashed_accounts.delete_current().unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: Some(account),
                        },
                    )
                    .unwrap();
            }

            // Don't touch the last third of genesis accounts

            // And add some new accounts, each holding 1 ETH
            for i in 3 * N..4 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).unwrap();
                account_change_table
                    .upsert(
                        block_key,
                        tables::AccountChange {
                            address,
                            account: None,
                        },
                    )
                    .unwrap();
            }

            let incremental_root =
                increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None).unwrap();

            let incremental_nodes = read_all_nodes(txn.cursor(tables::TrieAccount).unwrap());

            (incremental_root, incremental_nodes)
        };

        let (fused_root, fused_nodes) = {
            // ------------------------------------------------------------------------------
            // Take B: generate intermediate hashes for the accounts as of Block 1 in one go,
            // without increment_intermediate_hashes
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().unwrap();
            let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();

            // Accounts [0,N) now hold 2 ETH
            for i in 0..N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, two_eth).unwrap();
            }

            // Accounts [N,2N) are deleted

            // Accounts [2N,4N) hold 1 ETH
            for i in 2 * N..4 * N {
                let address = int_to_address(i);
                let hash = keccak256(address);
                hashed_accounts.upsert(hash, one_eth).unwrap();
            }

            let fused_root = regenerate_intermediate_hashes(&txn, &temp_dir, None).unwrap();

            let fused_nodes = read_all_nodes(txn.cursor(tables::TrieAccount).unwrap());

            (fused_root, fused_nodes)
        };

        // ------------------------------------------------------------------------------
        // A and B should yield the same result
        // ------------------------------------------------------------------------------
        assert_eq!(fused_root, incremental_root);
        assert_eq!(fused_nodes, incremental_nodes);
    }

    #[test]
    fn incremental_vs_regeneration_for_storage() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_chaindata().unwrap();

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

        fn upsert_storage_for_two_test_accounts<'tx>(
            hashed_storage: &mut MdbxCursor<'tx, RW, tables::HashedStorage>,
            storage_change_table: &mut MdbxCursor<'tx, RW, tables::StorageChangeSet>,
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
            .unwrap();
            upsert_hashed_storage_value(
                hashed_storage,
                keccak256(address2),
                keccak256(plain_loc2),
                value,
            )
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
                    .unwrap();
            }
        }
        let value_x = 0x42.as_u256();
        let value_y = U256::from_be_bytes(hex!(
            "71f602b294119bf452f1923814f5c6de768221254d3056b1bd63e72dc3142a29"
        ));
        {
            let txn = db.begin_mutable().unwrap();
            let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();

            hashed_accounts
                .upsert(keccak256(address1), account1)
                .unwrap();
            hashed_accounts
                .upsert(keccak256(address2), account2)
                .unwrap();

            drop(hashed_accounts);
            txn.commit().unwrap();
        }

        let (incremental_root, incremental_nodes) = {
            // ------------------------------------------------------------------------------
            // Take A: create some storage at genesis and then apply some changes at Block 1
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().unwrap();

            let mut hashed_storage = txn.cursor(tables::HashedStorage).unwrap();
            let mut storage_change_table = txn.cursor(tables::StorageChangeSet).unwrap();

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
                );
            }

            regenerate_intermediate_hashes(&txn, &temp_dir, None).unwrap();

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
                );
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
                );
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
                );
            }

            let incremental_root =
                increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None).unwrap();

            let incremental_nodes = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());

            (incremental_root, incremental_nodes)
        };

        let (fused_root, fused_nodes) = {
            // ------------------------------------------------------------------------------
            // Take B: generate intermediate hashes for the storage as of Block 1 in one go,
            // without increment_intermediate_hashes
            // ------------------------------------------------------------------------------

            let txn = db.begin_mutable().unwrap();
            let mut hashed_storage = txn.cursor(tables::HashedStorage).unwrap();
            let mut storage_change_table = txn.cursor(tables::StorageChangeSet).unwrap();

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
                );
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
                );
            }

            let fused_root = regenerate_intermediate_hashes(&txn, &temp_dir, None).unwrap();

            let fused_nodes = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());

            (fused_root, fused_nodes)
        };

        // ------------------------------------------------------------------------------
        // A and B should yield the same result
        // ------------------------------------------------------------------------------
        assert_eq!(fused_root, incremental_root);
        assert_eq!(fused_nodes, incremental_nodes);
    }

    #[test]
    fn storage_deletion() {
        let temp_dir = TempDir::new().unwrap();
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

        let address = hex!("1000000000000000000000000000000000000000").into();
        let hashed_address = keccak256(address);

        let account = Account {
            nonce: 1,
            balance: 15.as_u256() * ETHER,
            code_hash: hex!("7792ad513ce4d8f49163e21b25bf27ce6c8a0fa1e78c564e7d20a2d303303ba0")
                .into(),
        };

        let mut hashed_accounts = txn.cursor(tables::HashedAccount).unwrap();
        let mut hashed_storage = txn.cursor(tables::HashedStorage).unwrap();

        hashed_accounts.upsert(hashed_address, account).unwrap();

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
        .unwrap();
        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location2,
            value2,
        )
        .unwrap();
        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location3,
            value3,
        )
        .unwrap();

        regenerate_intermediate_hashes(&txn, &temp_dir, None).unwrap();

        // There should be one root node in storage trie
        let nodes_a = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());
        assert_eq!(nodes_a.len(), 1);

        drop(hashed_accounts);
        drop(hashed_storage);
        txn.commit().unwrap();

        {
            // Increment the trie without any changes
            let txn = db.begin_mutable().unwrap();
            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None).unwrap();
            let nodes_b = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());
            assert_eq!(nodes_b, nodes_a);
        }

        {
            // Delete storage and increment the trie
            let txn = db.begin_mutable().unwrap();
            let mut hashed_storage = txn.cursor(tables::HashedStorage).unwrap();
            let mut storage_change_table = txn.cursor(tables::StorageChangeSet).unwrap();

            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location1,
                U256::ZERO,
            )
            .unwrap();
            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location2,
                U256::ZERO,
            )
            .unwrap();
            upsert_hashed_storage_value(
                &mut hashed_storage,
                hashed_address,
                hashed_location3,
                U256::ZERO,
            )
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
                .unwrap();
            storage_change_table
                .upsert(
                    storage_change_key,
                    tables::StorageChange {
                        location: plain_location2,
                        value: U256::ZERO,
                    },
                )
                .unwrap();
            storage_change_table
                .upsert(
                    storage_change_key,
                    tables::StorageChange {
                        location: plain_location3,
                        value: U256::ZERO,
                    },
                )
                .unwrap();

            increment_intermediate_hashes(&txn, &temp_dir, BlockNumber(0), None).unwrap();
            let nodes_b = read_all_nodes(txn.cursor(tables::TrieStorage).unwrap());
            assert!(nodes_b.is_empty());
        }
    }

    #[test]
    fn test_intermediate_hashes_increment_key() {
        assert_eq!(increment_nibbled_key(&[]), None);
        assert_eq!(increment_nibbled_key(&[0x1, 0x2]), Some(vec![0x1, 0x3]));
        assert_eq!(increment_nibbled_key(&[0x1, 0xF]), Some(vec![0x2, 0x0]));
        assert_eq!(increment_nibbled_key(&[0xF, 0xF]), None);
        assert_eq!(
            increment_nibbled_key(&[0x1, 0x2, 0x0]),
            Some(vec![0x1, 0x2, 0x1])
        );
        assert_eq!(
            increment_nibbled_key(&[0x1, 0x2, 0xE]),
            Some(vec![0x1, 0x2, 0xF])
        );
        assert_eq!(
            increment_nibbled_key(&[0x1, 0x2, 0xF]),
            Some(vec![0x1, 0x3, 0x0])
        );
        assert_eq!(
            increment_nibbled_key(&[0x1, 0xF, 0xF]),
            Some(vec![0x2, 0x0, 0x0])
        );
        assert_eq!(increment_nibbled_key(&[0xF, 0xF, 0xF]), None);
    }

    #[test]
    fn test_regression_iterates_over_all_storage_changes() {
        let db = new_mem_chaindata().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let address = Address::from_low_u64_be(5);
        let hashed_address = keccak256(address);
        let account = Account {
            balance: 3.as_u256() * ETHER,
            ..Default::default()
        };

        let address_2 = Address::from_low_u64_be(8);
        let hashed_address_2 = keccak256(address_2);
        let account_2 = Account {
            balance: 15.as_u256() * ETHER,
            ..Default::default()
        };

        let address_3 = Address::from_low_u64_be(15);
        let hashed_address_3 = keccak256(address_3);

        let location_1: H256 =
            hex!("1000000000000000000000000000000000000000000000000000000000000000").into();
        let location_2: H256 =
            hex!("2000000000000000000000000000000000000000000000000000000000000000").into();

        let hashed_location_1 = keccak256(location_1.to_fixed_bytes());
        let hashed_location_2 = keccak256(location_2.to_fixed_bytes());

        let value_1 = 1.as_u256();
        let value_2 = 2.as_u256();

        let storage = BTreeMap::from([(location_1, value_1), (location_2, value_2)]);

        let state = BTreeMap::from([
            (address, (account, storage)),
            (address_2, (account_2, BTreeMap::new())),
            (address_3, (account_2, BTreeMap::new())),
        ]);

        let expected_root = expected_state_root(&state);

        let tx = db.begin_mutable().unwrap();

        // Create a couple of accounts to populate TrieAccount with a node
        let mut hashed_account = tx.cursor(tables::HashedAccount).unwrap();

        hashed_account.upsert(hashed_address, account).unwrap();
        hashed_account.upsert(hashed_address_2, account_2).unwrap();
        hashed_account.upsert(hashed_address_3, account_2).unwrap();

        regenerate_intermediate_hashes(&tx, &temp_dir, None).unwrap();

        // Change storage entries under that node
        // With the faulty gather_storage_changes() we would miss them.
        let mut hashed_storage = tx.cursor(tables::HashedStorage).unwrap();
        let mut storage_change_set = tx.cursor(tables::StorageChangeSet).unwrap();

        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location_1,
            value_1,
        )
        .unwrap();

        upsert_hashed_storage_value(
            &mut hashed_storage,
            hashed_address,
            hashed_location_2,
            value_2,
        )
        .unwrap();

        storage_change_set
            .upsert(
                StorageChangeKey {
                    block_number: BlockNumber(5),
                    address,
                },
                StorageChange {
                    location: hashed_location_2,
                    value: value_2,
                },
            )
            .unwrap();

        storage_change_set
            .upsert(
                StorageChangeKey {
                    block_number: BlockNumber(5),
                    address,
                },
                StorageChange {
                    location: hashed_location_1,
                    value: value_1,
                },
            )
            .unwrap();

        let root = increment_intermediate_hashes(&tx, &temp_dir, BlockNumber(2), None).unwrap();

        assert_eq!(root, expected_root);
    }

    // proptest strategies
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
                let mut b = BytesMut::new();
                Encodable::encode(&zeroless_view(&u256_to_h256(*v)), &mut b);
                (keccak256(k.to_fixed_bytes()), b)
            }))
        }
    }

    fn expected_state_root(accounts_with_storage: &BTreeMap<Address, (Account, Storage)>) -> H256 {
        trie_root(
            accounts_with_storage
                .iter()
                .map(|(&address, (account, storage))| {
                    let account_rlp = account.to_rlp(expected_storage_root(storage));
                    (keccak256(address), fastrlp::encode_fixed_size(&account_rlp))
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

    fn add_account_to_hashed_state<'tx, 'cu>(
        account_cursor: &'cu mut MdbxCursor<'tx, RW, tables::HashedAccount>,
        storage_cursor: &'cu mut MdbxCursor<'tx, RW, tables::HashedStorage>,
        address: &Address,
        account: &Account,
        storage: &Storage,
    ) -> Result<()>
    where
        'tx: 'cu,
    {
        let address_hash = keccak256(address);
        account_cursor.upsert(address_hash, *account)?;
        for (location, value) in storage {
            let location_hash = keccak256(location);
            storage_cursor.upsert(address_hash, (location_hash, *value))?
        }
        Ok(())
    }

    fn populate_hashed_state<'db, 'tx, E>(
        tx: &'tx MdbxTransaction<'db, RW, E>,
        accounts_with_storage: BTreeMap<Address, (Account, Storage)>,
    ) -> Result<()>
    where
        E: EnvironmentKind,
        'db: 'tx,
    {
        tx.clear_table(tables::HashedAccount)?;
        tx.clear_table(tables::HashedStorage)?;

        let mut account_cursor = tx.cursor(tables::HashedAccount)?;
        let mut storage_cursor = tx.cursor(tables::HashedStorage)?;

        for (address, (account, storage)) in accounts_with_storage {
            add_account_to_hashed_state(
                &mut account_cursor,
                &mut storage_cursor,
                &address,
                &account,
                &storage,
            )?;
        }

        Ok(())
    }

    fn populate_change_sets<'db, 'tx, E>(
        tx: &'tx MdbxTransaction<'db, RW, E>,
        changing_accounts: &BTreeMap<Address, ChangingAccount>,
    ) -> Result<()>
    where
        E: EnvironmentKind,
        'db: 'tx,
    {
        tx.clear_table(tables::AccountChangeSet)?;
        tx.clear_table(tables::StorageChangeSet)?;

        let mut account_cursor = tx.cursor(tables::AccountChangeSet)?;
        let mut storage_cursor = tx.cursor(tables::StorageChangeSet)?;

        for (address, states) in changing_accounts {
            let mut previous: Option<&(Account, Storage)> = None;
            for (height, current) in states {
                let block_number = BlockNumber(*height as u64);
                if current.as_ref() != previous {
                    let previous_account = previous.as_ref().map(|(a, _)| *a);
                    let current_account = current.as_ref().map(|(a, _)| *a);
                    if current_account != previous_account {
                        account_cursor.upsert(
                            block_number,
                            AccountChange {
                                address: *address,
                                account: previous_account,
                            },
                        )?;
                    }
                    let empty_storage = Storage::new();
                    let previous_storage =
                        previous.as_ref().map(|(_, s)| s).unwrap_or(&empty_storage);
                    let current_storage =
                        current.as_ref().map(|(_, s)| s).unwrap_or(&empty_storage);
                    for (location, value) in previous_storage {
                        if current_storage.get(location).unwrap_or(&U256::ZERO) != value {
                            storage_cursor.upsert(
                                StorageChangeKey {
                                    block_number,
                                    address: *address,
                                },
                                StorageChange {
                                    location: *location,
                                    value: *value,
                                },
                            )?;
                        }
                    }
                    for location in current_storage.keys() {
                        if !previous_storage.contains_key(location) {
                            storage_cursor.upsert(
                                StorageChangeKey {
                                    block_number,
                                    address: *address,
                                },
                                StorageChange {
                                    location: *location,
                                    value: U256::ZERO,
                                },
                            )?;
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
    fn do_trie_root_matches(test_data: ChangingAccountsFixture) {
        let db = new_mem_chaindata().unwrap();
        let temp_dir = TempDir::new().unwrap();

        let tx = db.begin_mutable().unwrap();
        let state_before_increment = accounts_at_height(&test_data, test_data.before_increment);
        let expected = expected_state_root(&state_before_increment);
        populate_hashed_state(&tx, state_before_increment).unwrap();
        tx.commit().unwrap();

        let tx = db.begin_mutable().unwrap();
        let root = regenerate_intermediate_hashes(&tx, &temp_dir, None).unwrap();
        tx.commit().unwrap();

        assert_eq!(root, expected);

        let tx = db.begin_mutable().unwrap();
        let state_after_increment = accounts_at_height(&test_data, test_data.after_increment);
        let expected = expected_state_root(&state_after_increment);
        populate_hashed_state(&tx, state_after_increment).unwrap();
        populate_change_sets(&tx, &test_data.accounts).unwrap();
        tx.commit().unwrap();

        let tx = db.begin_mutable().unwrap();
        let root = increment_intermediate_hashes(
            &tx,
            &temp_dir,
            BlockNumber(test_data.before_increment as u64),
            None,
        )
        .unwrap();

        assert_eq!(root, expected);
    }

    proptest! {
        #[test]
        fn trie_root_matches(test_data in test_datas()) {
            do_trie_root_matches(test_data);
        }
    }
}
