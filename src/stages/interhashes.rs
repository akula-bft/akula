use crate::{
    accessors,
    crypto::keccak256,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::{
        tables,
        traits::{Cursor, CursorDupSort, Table},
        TableEncode,
    },
    models::{Account, BlockNumber, Incarnation, RlpAccount, EMPTY_ROOT},
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput},
    zeroless_view, MutableTransaction, StageId, Transaction,
};
use anyhow::*;
use async_trait::async_trait;
use ethereum_types::*;
use rlp::RlpStream;
use std::{cmp, collections::BTreeMap, marker::PhantomData};
use tracing::*;

fn hex_prefix(nibbles: &[u8], user_flag: bool) -> Vec<u8> {
    let odd_length = nibbles.len() & 1 != 0;

    let mut first_byte = 32 * (user_flag as u8);
    if odd_length {
        first_byte += 16 + nibbles[0];
    };

    let mut result = vec![first_byte];

    let remaining = if odd_length {
        nibbles[1..].chunks(2)
    } else {
        nibbles.chunks(2)
    };

    for chunk in remaining {
        result.push(chunk[0] * 16 + chunk[1]);
    }

    result
}

#[derive(Clone, Copy, Debug, derive_more::Deref)]
struct Nibbles(H512);

impl From<H256> for Nibbles {
    fn from(b: H256) -> Self {
        let mut n = [0; 64];
        for i in 0..b.as_fixed_bytes().len() {
            n[i * 2] = b[i] / 0x10;
            n[i * 2 + 1] = b[i] & 0x0f;
        }
        Self(n.into())
    }
}

type HashedAccountFusedValue = <tables::HashedAccount as Table>::FusedValue;

#[derive(Debug)]
struct BranchInProgress {
    slot: u8,
    key_part: Vec<u8>,
    data: Vec<u8>,
    is_leaf: bool,
}

fn keccak256_or_self(stream: RlpStream) -> Vec<u8> {
    let out = stream.out();
    if out.len() >= 32 {
        keccak256(out).as_ref().to_vec()
    } else {
        out.to_vec()
    }
}

impl BranchInProgress {
    fn stream(&self) -> RlpStream {
        let encoded_key_part = hex_prefix(&self.key_part, self.is_leaf);
        let mut stream = RlpStream::new_list(2);
        stream.append(&encoded_key_part);
        if !self.is_leaf && self.data.len() < 32 {
            stream.append_raw(&self.data, 1);
        } else {
            stream.append(&self.data);
        }
        stream
    }

    fn hash(&self) -> H256 {
        keccak256(self.stream().out())
    }

    fn hash_or_self(&self) -> Vec<u8> {
        keccak256_or_self(self.stream())
    }
}

#[derive(Debug)]
struct NodeInProgress {
    branches: Vec<BranchInProgress>,
}

impl NodeInProgress {
    fn new() -> Self {
        NodeInProgress {
            branches: Vec::with_capacity(16),
        }
    }

    fn set(&mut self, slot: u8, key_part: Vec<u8>, data: Vec<u8>, is_leaf: bool) {
        self.branches.push(BranchInProgress {
            slot,
            key_part,
            data,
            is_leaf,
        });
    }

    fn new_with_branch(slot: u8, key_part: Vec<u8>, data: Vec<u8>, is_leaf: bool) -> Self {
        let mut node = Self::new();
        node.set(slot, key_part, data, is_leaf);
        node
    }

    fn is_branch_node(&self) -> bool {
        self.branches.len() > 1
    }

    fn get_single_branch(&mut self) -> BranchInProgress {
        self.branches.pop().unwrap()
    }

    fn get_root_branch(&mut self) -> &BranchInProgress {
        let branch = &mut self.branches[0];
        branch.key_part.insert(0, branch.slot);
        branch
    }
}

#[derive(Debug, PartialEq)]
struct BranchNode {
    hashes: Vec<Option<Vec<u8>>>,
}

impl BranchNode {
    fn new() -> Self {
        Self {
            hashes: vec![None; 16],
        }
    }

    fn new_with_hashes(hashes: Vec<Option<Vec<u8>>>) -> Self {
        Self { hashes }
    }

    fn from(node: &NodeInProgress) -> Self {
        let mut new_node = Self::new();
        for branch in &node.branches {
            let child_is_branch_node = branch.key_part.is_empty() && !branch.is_leaf;
            let hash = if child_is_branch_node {
                branch.data.clone()
            } else {
                branch.hash_or_self()
            };
            new_node.hashes[branch.slot as usize] = Some(hash);
        }
        new_node
    }

    fn serialize(&self) -> Vec<u8> {
        // TODO handle the case of non-hash entries (affects storage trie only)
        let mut flags = 0u16;
        let mut result = vec![0u8, 0u8];
        for (slot, ref maybe_hash) in self.hashes.iter().enumerate() {
            if let Some(hash) = maybe_hash {
                flags += 1u16.checked_shl(slot as u32).unwrap();
                result.extend_from_slice(hash);
            }
        }
        let flag_bytes = flags.to_be_bytes();
        result[0] = flag_bytes[0];
        result[1] = flag_bytes[1];
        result
    }

    fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        let flags: u16 = data[0] as u16 * 0x100 + data[1] as u16;
        let mut index: usize = 2;
        let mut node = Self::new();
        for i in 0..16 {
            if flags & 1u16.checked_shl(i).unwrap() != 0 {
                node.hashes[i as usize] = Some(data[index..index + 32].to_vec());
                index += 32;
            }
        }
        Ok(node)
    }

    fn stream(&self) -> RlpStream {
        let mut stream = RlpStream::new_list(17);
        for maybe_hash in &self.hashes {
            match maybe_hash {
                Some(ref hash) => {
                    if hash.len() == 32 {
                        stream.append(hash)
                    } else {
                        stream.append_raw(hash, 1)
                    }
                }
                None => stream.append_empty_data(),
            };
        }
        stream.append_empty_data();
        stream
    }

    fn hash(&self) -> H256 {
        keccak256(self.stream().out())
    }

    fn hash_or_self(&self) -> Vec<u8> {
        keccak256_or_self(self.stream())
    }

    fn is_proper_branch_node(&self) -> bool {
        self.hashes.iter().filter(|h| h.is_some()).count() > 1
    }
}

trait CollectToTrie {
    fn collect(&mut self, key: Vec<u8>, value: Vec<u8>);
}

struct StateTrieCollector<'co> {
    collector: &'co mut Collector<tables::TrieAccount>,
}

impl<'co> CollectToTrie for StateTrieCollector<'co> {
    fn collect(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.collector.collect(Entry::new(key, value));
    }
}

struct StorageTrieCollector<'co> {
    collector: &'co mut Collector<tables::TrieStorage>,
    path_prefix: Vec<u8>,
}

impl<'co> StorageTrieCollector<'co> {
    fn new(
        collector: &'co mut Collector<tables::TrieStorage>,
        hashed_address: H256,
        incarnation: Incarnation,
    ) -> Self {
        Self {
            collector,
            path_prefix: TableEncode::encode((hashed_address, incarnation)).to_vec(),
        }
    }
}

impl<'co> CollectToTrie for StorageTrieCollector<'co> {
    fn collect(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut full_key = self.path_prefix.clone();
        full_key.extend_from_slice(&key);
        self.collector.collect(Entry::new(full_key, value));
    }
}

struct TrieBuilder<C>
where
    C: CollectToTrie,
{
    in_progress: BTreeMap<Vec<u8>, NodeInProgress>,
    visited: Vec<u8>,
    last_prefix_length: usize,
    collect: C,
}

impl<C> TrieBuilder<C>
where
    C: CollectToTrie,
{
    fn new(collect: C) -> Self {
        Self {
            in_progress: BTreeMap::new(),
            visited: vec![],
            last_prefix_length: 63,
            collect,
        }
    }

    fn add_branch(
        &mut self,
        at: Vec<u8>,
        slot: u8,
        key_part: Vec<u8>,
        data: Vec<u8>,
        is_leaf: bool,
    ) {
        if let Some(node) = self.in_progress.get_mut(&at) {
            node.set(slot, key_part, data, is_leaf);
        } else {
            let new_node = NodeInProgress::new_with_branch(slot, key_part, data, is_leaf);
            self.in_progress.insert(at, new_node);
        }
    }

    fn node_in_range(&mut self, prev_key: Option<Nibbles>) -> Option<(Vec<u8>, NodeInProgress)> {
        if let Some(key) = self.in_progress.keys().last() {
            if key.as_slice() > prev_key.as_ref().map(|k| k.as_bytes()).unwrap_or(&[]) {
                return self.in_progress.pop_last();
            }
        }
        None
    }

    fn prefix_length(&mut self, current_key: Nibbles, prev_key: Option<Nibbles>) -> usize {
        if let Some(prev_key) = prev_key {
            let mut i = 0;
            while current_key[i] == prev_key[i] {
                i += 1;
            }
            let length = cmp::max(i, self.last_prefix_length);
            self.last_prefix_length = i;
            length
        } else {
            self.last_prefix_length
        }
    }

    fn visit_leaf(&mut self, key: Nibbles, data: Vec<u8>, prefix_length: usize) {
        let prefix_length = if prefix_length < 63 {
            prefix_length + 1
        } else {
            prefix_length
        };
        let prefix = key[..prefix_length].to_vec();
        let infix = key[prefix_length];
        let suffix = key[prefix_length + 1..].to_vec();
        self.add_branch(prefix, infix, suffix, data, true);
    }

    fn finalize_branch_node(&mut self, key: &[u8], node: &NodeInProgress) -> Vec<u8> {
        let branch_node = BranchNode::from(node);
        let serialized = branch_node.serialize();
        self.collect.collect(key.to_vec(), serialized);
        branch_node.hash_or_self()
    }

    fn visit_node(&mut self, key: Vec<u8>, mut node: NodeInProgress) {
        let mut parent_key = key.clone();
        let parent_slot = parent_key.pop().unwrap();
        if node.is_branch_node() {
            let hash = self.finalize_branch_node(&key, &node);
            self.add_branch(parent_key, parent_slot, vec![], hash, false);
        } else {
            let BranchInProgress {
                slot,
                mut key_part,
                data,
                is_leaf,
            } = node.get_single_branch();
            key_part.insert(0, slot);
            self.add_branch(parent_key, parent_slot, key_part, data, is_leaf);
        }
    }

    fn handle_range(
        &mut self,
        current_key: Nibbles,
        current_value: Vec<u8>,
        prev_key: Option<Nibbles>,
    ) {
        let prefix_length = self.prefix_length(current_key, prev_key);
        self.visit_leaf(current_key, current_value, prefix_length);
        while let Some((key, node)) = self.node_in_range(prev_key) {
            self.visit_node(key, node);
        }
    }

    fn get_root(&mut self) -> H256 {
        if let Some(root_node) = self.in_progress.get_mut(&vec![]) {
            if root_node.is_branch_node() {
                BranchNode::from(root_node).hash()
            } else {
                root_node.get_root_branch().hash()
            }
        } else {
            EMPTY_ROOT
        }
    }
}

fn build_storage_trie(
    collector: &mut Collector<tables::TrieStorage>,
    address_hash: H256,
    incarnation: Incarnation,
    storage: &[(H256, H256)],
) -> H256 {
    if storage.is_empty() {
        return EMPTY_ROOT;
    }

    let wrapped_collector = StorageTrieCollector::new(collector, address_hash, incarnation);
    let mut builder = TrieBuilder::new(wrapped_collector);

    let mut storage_iter = storage.iter().rev();
    let mut current = storage_iter.next();

    while let Some(&(location, value)) = &mut current {
        let current_value = zeroless_view(&value);
        let current_key = location.into();
        let prev = storage_iter.next();

        let prev_key = prev.map(|&(v, _)| v.into());

        let data = rlp::encode(&current_value).to_vec();

        builder.handle_range(current_key, data, prev_key);

        current = prev;
    }

    builder.get_root()
}

struct GenerateWalker<'db: 'tx, 'tx: 'co, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    cursor: RwTx::Cursor<'tx, tables::HashedAccount>,
    storage_cursor: RwTx::CursorDupSort<'tx, tables::HashedStorage>,
    trie_builder: TrieBuilder<StateTrieCollector<'co>>,
    storage_collector: &'co mut Collector<tables::TrieStorage>,
}

impl<'db: 'tx, 'tx: 'co, 'co, RwTx> GenerateWalker<'db, 'tx, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    async fn new(
        tx: &'tx mut RwTx,
        collector: &'co mut Collector<tables::TrieAccount>,
        storage_collector: &'co mut Collector<tables::TrieStorage>,
    ) -> anyhow::Result<GenerateWalker<'db, 'tx, 'co, RwTx>>
    where
        RwTx: MutableTransaction<'db>,
    {
        let mut cursor = tx.cursor(&tables::HashedAccount).await?;
        let storage_cursor = tx.cursor_dup_sort(&tables::HashedStorage).await?;
        cursor.last().await?;
        let trie_builder = TrieBuilder::new(StateTrieCollector { collector });

        Ok(GenerateWalker {
            cursor,
            storage_cursor,
            trie_builder,
            storage_collector,
        })
    }

    async fn do_get_prev_account(
        &mut self,
        value: Option<HashedAccountFusedValue>,
    ) -> anyhow::Result<Option<(H256, RlpAccount)>> {
        if let Some(fused_value) = value {
            let (address_hash, encoded_account) = fused_value;
            let account = Account::decode_for_storage(encoded_account.as_ref())?.unwrap();
            let storage_root = self
                .visit_storage(address_hash, account.incarnation)
                .await?;
            Ok(Some((address_hash, account.to_rlp(storage_root))))
        } else {
            Ok(None)
        }
    }

    async fn get_prev_account(&mut self) -> anyhow::Result<Option<(H256, RlpAccount)>> {
        let init_value = self.cursor.prev().await?;
        self.do_get_prev_account(init_value).await
    }

    async fn get_last_account(&mut self) -> anyhow::Result<Option<(H256, RlpAccount)>> {
        let init_value = self.cursor.last().await?;
        self.do_get_prev_account(init_value).await
    }

    async fn storage_for_account(
        &mut self,
        address_hash: H256,
        incarnation: Incarnation,
    ) -> anyhow::Result<Vec<(H256, H256)>> {
        let mut storage = Vec::<(H256, H256)>::new();
        let mut found = self
            .storage_cursor
            .seek((address_hash, incarnation))
            .await?;
        while let Some((_, storage_entry)) = found {
            storage.push((storage_entry.0, (storage_entry.1).0));
            found = self.storage_cursor.next_dup().await?;
        }
        Ok(storage)
    }

    async fn visit_storage(
        &mut self,
        address_hash: H256,
        incarnation: Incarnation,
    ) -> anyhow::Result<H256> {
        let storage = self.storage_for_account(address_hash, incarnation).await?;
        let storage_root =
            build_storage_trie(self.storage_collector, address_hash, incarnation, &storage);
        Ok(storage_root)
    }

    fn handle_range(
        &mut self,
        current_key: Nibbles,
        account: &RlpAccount,
        prev_key: Option<Nibbles>,
    ) {
        self.trie_builder
            .handle_range(current_key, rlp::encode(account).to_vec(), prev_key);
    }

    fn save_root(&mut self) {
        let maybe_root_node = self
            .trie_builder
            .in_progress
            .get(&vec![])
            .map(BranchNode::from);
        if let Some(node) = maybe_root_node {
            if node.is_proper_branch_node() {
                self.trie_builder.collect.collect(vec![], node.serialize());
            }
        }
    }

    fn get_root(&mut self) -> H256 {
        self.trie_builder.get_root()
    }
}

async fn generate_interhashes<'db: 'tx, 'tx, RwTx>(
    tx: &mut RwTx,
    collector: &mut Collector<tables::TrieAccount>,
    storage_collector: &mut Collector<tables::TrieStorage>,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let mut walker = GenerateWalker::new(tx, collector, storage_collector).await?;
    let mut current = walker.get_last_account().await?;

    while let Some((hashed_account_key, account_data)) = current {
        let upcoming = walker.get_prev_account().await?;
        let current_key = hashed_account_key.into();
        let upcoming_key = upcoming.as_ref().map(|&(key, _)| key.into());
        walker.handle_range(current_key, &account_data, upcoming_key);
        current = upcoming;
    }

    walker.save_root();
    Ok(walker.get_root())
}

#[derive(Debug)]
struct ChangedAccount {
    new_value: RlpAccount,
    created: bool,
}

impl ChangedAccount {
    fn new(new_value: RlpAccount, created: bool) -> Self {
        Self { new_value, created }
    }
}

async fn collect_changed_accounts<'db: 'tx, 'tx, Tx>(
    tx: &'tx Tx,
    from: BlockNumber,
    to: BlockNumber,
) -> anyhow::Result<BTreeMap<Address, bool>>
where
    Tx: Transaction<'db>,
{
    let mut changed = BTreeMap::new();
    let mut cursor = tx.cursor_dup_sort(&tables::AccountChangeSet).await?;

    let mut fused = cursor.seek(from).await?;

    while let Some((block_number, ref change)) = fused {
        if block_number > to {
            break;
        }
        changed
            .entry(change.address)
            .or_insert_with(|| change.account.is_empty());
        fused = cursor.next().await?;
    }

    Ok(changed)
}

async fn collect_changes_to_accounts<'db: 'tx, 'tx, Tx>(
    tx: &'tx Tx,
    changed: BTreeMap<Address, bool>,
) -> anyhow::Result<BTreeMap<H256, ChangedAccount>>
where
    Tx: Transaction<'db>,
{
    let mut result = BTreeMap::new();

    for (address, created) in changed.iter() {
        let address_hash = keccak256(address);
        let account_raw = tx.get(&tables::HashedAccount, address_hash).await?.unwrap();
        let account = Account::decode_for_storage(&account_raw)?.unwrap();
        let storage_root = EMPTY_ROOT; // TODO
        let account_change = ChangedAccount::new(account.to_rlp(storage_root), *created);
        result.insert(address_hash, account_change);
    }

    Ok(result)
}

async fn collect_changes<'db: 'tx, 'tx, Tx>(
    tx: &'tx Tx,
    from: BlockNumber,
    to: BlockNumber,
) -> anyhow::Result<BTreeMap<H256, ChangedAccount>>
where
    Tx: Transaction<'db>,
{
    let changed = collect_changed_accounts(tx, from, to).await?;
    collect_changes_to_accounts(tx, changed).await
}

struct TrieUpdater<C>
where
    C: CollectToTrie,
{
    in_progress: BTreeMap<Vec<u8>, BranchNode>,
    collect: C,
    root_hash: Option<H256>,
}

fn compute_root_hash(path: &[u8], node: &BranchNode) -> H256 {
    if path.is_empty() {
        node.hash()
    } else {
        let mut extension_root_node = NodeInProgress::new_with_branch(
            path[0],
            path[1..].to_vec(),
            node.hash_or_self(),
            false,
        );
        extension_root_node.get_root_branch().hash()
    }
}

impl<C> TrieUpdater<C>
where
    C: CollectToTrie,
{
    fn new(collect: C) -> Self {
        Self {
            in_progress: BTreeMap::new(),
            collect,
            root_hash: None,
        }
    }

    fn has_node(&self, at: &[u8]) -> bool {
        self.in_progress.contains_key(at)
    }

    fn add_node(&mut self, at: Vec<u8>, node_raw: &[u8]) -> anyhow::Result<bool> {
        let is_new = !self.has_node(&at);
        if is_new {
            self.in_progress
                .insert(at, BranchNode::deserialize(node_raw)?);
        }
        Ok(is_new)
    }

    fn add_branch(&mut self, at: &[u8], branch: BranchInProgress) -> bool {
        let node = self.in_progress.get_mut(at).unwrap();
        let slot = branch.slot as usize;
        let is_new_branch = node.hashes[slot].is_none();
        if !is_new_branch {
            let hash = if branch.key_part.is_empty() {
                branch.data
            } else {
                branch.hash_or_self()
            };
            node.hashes[slot] = Some(hash);
        }
        is_new_branch
    }

    fn update_parent(&mut self, path: &[u8], changed_hash: H256) {
        let mut parent_path = path.to_vec();
        let mut key_part = vec![];
        let slot = loop {
            if let Some(last_nibble) = parent_path.pop() {
                if self.in_progress.contains_key(&parent_path) {
                    break last_nibble;
                } else {
                    key_part.insert(0, last_nibble);
                }
            } else {
                return;
            }
        };
        let branch = BranchInProgress {
            slot,
            key_part,
            data: changed_hash.as_bytes().to_vec(),
            is_leaf: false,
        };
        self.add_branch(&parent_path, branch);
    }

    fn find_in_range(&mut self, boundary: &[u8]) -> Option<(Vec<u8>, BranchNode)> {
        if let Some(entry) = self.in_progress.last_entry() {
            let path = entry.key();
            if path.as_slice() >= boundary {
                let path = path.to_vec();
                return Some((path, entry.remove()));
            }
        }
        None
    }

    fn finalize_range(&mut self, boundary: Option<Nibbles>) {
        let boundary = boundary.as_ref().map(|n| n.as_bytes()).unwrap_or(&[]);
        while let Some((path, node)) = self.find_in_range(boundary) {
            self.update_parent(&path, node.hash());
            if self.in_progress.is_empty() {
                self.root_hash = Some(compute_root_hash(&path, &node));
            }
            self.collect.collect(path, node.serialize());
        }
    }

    fn set_unchanged_root_hash(&mut self) {
        let (path, node) = self.in_progress.first_key_value().unwrap();
        self.root_hash = Some(compute_root_hash(path, node));
    }
}

fn is_parent_key(maybe_parent: &[u8], child: Nibbles) -> bool {
    &child.0[0..maybe_parent.len()] == maybe_parent
}

struct UpdateWalker<'db: 'tx, 'tx: 'co, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    tx: &'tx mut RwTx,
    trie_updater: TrieUpdater<StateTrieCollector<'co>>,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx: 'co, 'co, RwTx> UpdateWalker<'db, 'tx, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    fn new(
        tx: &'tx mut RwTx,
        collector: &'co mut Collector<tables::TrieAccount>,
    ) -> UpdateWalker<'db, 'tx, 'co, RwTx>
    where
        RwTx: MutableTransaction<'db>,
    {
        let trie_updater = TrieUpdater::new(StateTrieCollector { collector });
        UpdateWalker {
            tx,
            trie_updater,
            _marker: PhantomData,
        }
    }

    fn insert(&mut self, path: Vec<u8>, node_raw: &[u8]) -> anyhow::Result<()> {
        self.trie_updater.add_node(path, node_raw)?;
        Ok(())
    }

    async fn retrieve_parent(&mut self, leaf_at: Nibbles) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let (parent_key, parent_data) = {
            let mut cursor = self.tx.mutable_cursor(&tables::TrieAccount).await?;
            cursor.seek(leaf_at.as_bytes().to_vec()).await?;
            cursor.prev().await?.unwrap()
        };

        if !is_parent_key(&parent_key, leaf_at) {
            let mut possible_parent = leaf_at.0[0..=parent_key.len()].to_vec();
            while possible_parent.pop().is_some() {
                if let Some(data) = self
                    .tx
                    .get(&tables::TrieAccount, possible_parent.clone())
                    .await?
                {
                    return Ok((possible_parent, data));
                }
            }
            unreachable!("At least root must be in TrieAccount table if this path is executed.");
        }

        Ok((parent_key, parent_data))
    }

    async fn retrieve_ancestors(&mut self, mut path: Vec<u8>) -> anyhow::Result<()> {
        while !path.is_empty() {
            path.pop();
            if self.trie_updater.has_node(&path) {
                continue;
            }
            if let Some(node_raw) = self.tx.get(&tables::TrieAccount, path.clone()).await? {
                self.trie_updater.add_node(path.clone(), &node_raw)?;
            }
        }
        Ok(())
    }

    async fn handle_range(
        &mut self,
        leaf_at: Nibbles,
        data: Vec<u8>,
        prev_leaf_at: Option<Nibbles>,
    ) -> anyhow::Result<()> {
        let (parent_path, parent_node) = self.retrieve_parent(leaf_at).await?;
        let is_new_entry = self
            .trie_updater
            .add_node(parent_path.clone(), &parent_node)?;
        let slot = leaf_at[parent_path.len()];
        let changed_branch = BranchInProgress {
            slot,
            key_part: leaf_at[parent_path.len() + 1..].to_vec(),
            data,
            is_leaf: true,
        };
        self.trie_updater.add_branch(&parent_path, changed_branch);
        if is_new_entry {
            self.retrieve_ancestors(parent_path).await?;
        }
        self.trie_updater.finalize_range(prev_leaf_at);
        Ok(())
    }

    fn root_hash(&self) -> Option<H256> {
        self.trie_updater.root_hash
    }
}

fn pop_last_change(
    changes: &mut BTreeMap<H256, ChangedAccount>,
) -> (Option<Nibbles>, Option<ChangedAccount>) {
    match changes.pop_last() {
        Some((p, c)) => (Some(Nibbles::from(p)), Some(c)),
        None => (None, None),
    }
}

async fn do_update_interhashes<'db: 'tx, 'tx, RwTx>(
    tx: &'tx mut RwTx,
    collector: &mut Collector<tables::TrieAccount>,
    from: BlockNumber,
    to: BlockNumber,
    top_node_path: Vec<u8>,
    top_node_raw: &[u8],
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let mut changes = collect_changes(tx, from, to).await?;
    let mut walker = UpdateWalker::new(tx, collector);
    walker.insert(top_node_path, top_node_raw)?;

    if changes.is_empty() {
        walker.trie_updater.set_unchanged_root_hash();
        return Ok(walker.root_hash().unwrap());
    }

    let (mut current_path, mut current_change) = pop_last_change(&mut changes);

    loop {
        let (previous_path, previous_change) = pop_last_change(&mut changes);

        let change = current_change.unwrap();
        let data = rlp::encode(&change.new_value).to_vec();
        if change.created {
            todo!();
        } else {
            walker
                .handle_range(current_path.unwrap(), data, previous_path)
                .await?;
        }

        if previous_path.is_none() {
            break;
        }

        current_path = previous_path;
        current_change = previous_change;
    }

    Ok(walker.root_hash().unwrap())
}

async fn update_interhashes<'db: 'tx, 'tx, RwTx>(
    tx: &'tx mut RwTx,
    collector: &mut Collector<tables::TrieAccount>,
    storage_collector: &mut Collector<tables::TrieStorage>,
    from: BlockNumber,
    to: BlockNumber,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let _ = from;
    let _ = to;

    tx.clear_table(&tables::TrieAccount).await?;
    tx.clear_table(&tables::TrieStorage).await?;

    generate_interhashes(tx, collector, storage_collector).await
}

async fn update_interhashes2<'db: 'tx, 'tx, RwTx>(
    tx: &'tx mut RwTx,
    collector: &mut Collector<tables::TrieAccount>,
    storage_collector: &mut Collector<tables::TrieStorage>,
    from: BlockNumber,
    to: BlockNumber,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let topmost_entry = {
        let mut cursor = tx.cursor(&tables::TrieAccount).await?;
        cursor.first().await?
    };

    match topmost_entry {
        Some((path, raw_node)) => {
            do_update_interhashes(tx, collector, from, to, path, &raw_node).await
        }
        None => generate_interhashes(tx, collector, storage_collector).await,
    }
}

async fn load_results<'db: 'tx, 'tx: 'co, 'co, RwTx>(
    tx: &'tx mut RwTx,
    collector: &'co mut Collector<tables::TrieAccount>,
    storage_collector: &'co mut Collector<tables::TrieStorage>,
) -> anyhow::Result<()>
where
    RwTx: MutableTransaction<'db>,
{
    let mut write_cursor = tx.mutable_cursor(&tables::TrieAccount.erased()).await?;
    collector.load(&mut write_cursor).await?;
    let mut storage_write_cursor = tx.mutable_cursor(&tables::TrieStorage.erased()).await?;
    storage_collector.load(&mut storage_write_cursor).await?;
    Ok(())
}

#[derive(Debug)]
pub struct Interhashes;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for Interhashes
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("Interhashes")
    }

    fn description(&self) -> &'static str {
        "Generating intermediate hashes for efficient computation of the trie root"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let past_progress = input.stage_progress.unwrap_or(BlockNumber(0));
        let prev_progress = input
            .previous_stage
            .map(|tuple| tuple.1)
            .unwrap_or(BlockNumber(0));

        if prev_progress > past_progress {
            let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
            let mut storage_collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

            let trie_root = if past_progress == BlockNumber(0) {
                generate_interhashes(tx, &mut collector, &mut storage_collector)
                    .await
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                update_interhashes(
                    tx,
                    &mut collector,
                    &mut storage_collector,
                    past_progress,
                    prev_progress,
                )
                .await
                .with_context(|| "Failed to update interhashes")?
            };

            let block_state_root = accessors::chain::header::read(
                tx,
                accessors::chain::canonical_hash::read(tx, prev_progress)
                    .await?
                    .ok_or_else(|| anyhow!("No canonical hash for block {}", prev_progress))?,
                prev_progress,
            )
            .await?
            .ok_or_else(|| anyhow!("No header for block {}", prev_progress))?
            .state_root;

            if block_state_root == trie_root {
                info!("Block #{} state root OK: {:?}", prev_progress, trie_root)
            } else {
                bail!(
                    "Block #{} state root mismatch: {:?} != {:?}",
                    prev_progress,
                    trie_root,
                    block_state_root
                )
            }

            load_results(tx, &mut collector, &mut storage_collector).await?;
        };

        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(prev_progress, past_progress),
            done: true,
            must_commit: true,
        })
    }

    async fn unwind<'tx>(&self, tx: &'tx mut RwTx, input: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::trie_root,
        kv::{
            tables::AccountChange,
            traits::{MutableCursorDupSort, MutableKV},
        },
        models::EMPTY_HASH,
        new_mem_database,
    };
    use ethereum_types::{Address, H256, U256};
    use hex_literal::hex;
    use proptest::prelude::*;
    use std::ops::Range;

    #[test]
    fn test_hex_prefix() {
        assert_eq!(
            hex_prefix(&[0x01, 0x02, 0x03, 0x04, 0x05], true),
            [0x31, 0x23, 0x45]
        );
        assert_eq!(hex_prefix(&[0x03, 0x02, 0x01], false), [0x13, 0x21]);
        assert_eq!(
            hex_prefix(&[0x02, 0x0a, 0x04, 0x02], false),
            [0x00, 0x2a, 0x42]
        );
        assert_eq!(hex_prefix(&[0x0f], true), [0x3f]);
        assert_eq!(
            hex_prefix(&[0x05, 0x08, 0x02, 0x02, 0x05, 0x01], true),
            [0x20, 0x58, 0x22, 0x51]
        );
    }

    fn addresses() -> impl Strategy<Value = Address> {
        any::<[u8; 20]>().prop_map(Address::from)
    }

    fn h256s() -> impl Strategy<Value = H256> {
        any::<[u8; 32]>().prop_map(H256::from)
    }

    prop_compose! {
        fn accounts()(
            nonce in any::<u64>(),
            balance in any::<[u8; 32]>(),
            code_hash in any::<[u8; 32]>(),
            incarnation in any::<u64>(),
        ) -> Account {
            let balance = U256::from(balance);
            let code_hash = H256::from(code_hash);
            let incarnation = Incarnation::from(incarnation);
            Account { nonce, balance, code_hash, incarnation }
        }
    }

    fn maps_of_accounts_plain() -> impl Strategy<Value = BTreeMap<Address, Account>> {
        prop::collection::btree_map(addresses(), accounts(), 1..500)
    }

    fn maps_of_accounts() -> impl Strategy<Value = BTreeMap<H256, Account>> {
        prop::collection::btree_map(h256s(), accounts(), 1..500)
    }

    fn accounts_plain_to_hashed(plain: &BTreeMap<Address, Account>) -> BTreeMap<H256, Account> {
        let mut hashed = BTreeMap::new();
        for (address, account) in plain {
            hashed.insert(keccak256(address), account.clone());
        }
        hashed
    }

    fn expected_root(accounts: &BTreeMap<H256, Account>) -> H256 {
        trie_root(accounts.iter().map(|(&address_hash, account)| {
            let account_rlp = account.to_rlp(EMPTY_ROOT);
            (address_hash, rlp::encode(&account_rlp))
        }))
    }

    async fn populate_hashed_accounts<Db>(db: &Db, accounts: &BTreeMap<H256, Account>)
    where
        Db: MutableKV,
    {
        let tx = db.begin_mutable().await.unwrap();

        for (address_hash, account_model) in accounts {
            let account = account_model.encode_for_storage(false);
            let fused_value = (*address_hash, account);
            tx.set(&tables::HashedAccount, fused_value).await.unwrap();
        }

        tx.commit().await.unwrap();
    }

    async fn call_generate_interhashes<Db>(db: &Db, results_are_used: bool) -> H256
    where
        Db: MutableKV,
    {
        let mut tx = db.begin_mutable().await.unwrap();

        let mut collector = Collector::<tables::TrieAccount>::new(OPTIMAL_BUFFER_CAPACITY);
        let mut storage_collector = Collector::<tables::TrieStorage>::new(OPTIMAL_BUFFER_CAPACITY);

        let root = generate_interhashes(&mut tx, &mut collector, &mut storage_collector)
            .await
            .unwrap();

        if results_are_used {
            load_results(&mut tx, &mut collector, &mut storage_collector)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        root
    }

    async fn do_root_matches(accounts: BTreeMap<H256, Account>) {
        let db = new_mem_database().unwrap();
        populate_hashed_accounts(&db, &accounts).await;

        let actual = call_generate_interhashes(&db, false).await;
        let expected = expected_root(&accounts);

        assert_eq!(actual, expected);
    }

    proptest! {
        #[test]
        fn root_matches(accounts in maps_of_accounts()) {
            tokio_test::block_on(do_root_matches(accounts));
        }
    }

    #[tokio::test]
    async fn root_matches_case_no_accounts() {
        let accounts = BTreeMap::new();
        do_root_matches(accounts).await
    }

    #[tokio::test]
    async fn root_matches_case_one_account() {
        let mut accounts = BTreeMap::new();
        let account1 = Account {
            nonce: 0,
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
            incarnation: Incarnation(0),
        };
        accounts.insert(H256::from_low_u64_be(1), account1);
        do_root_matches(accounts).await
    }

    #[tokio::test]
    async fn root_matches_case_consecutive_branch_nodes() {
        let mut accounts = BTreeMap::new();
        let a1 = Account {
            nonce: 6685434669699468178,
            balance: U256::from(13764859329281365277u128),
            code_hash: EMPTY_HASH,
            incarnation: Incarnation(38),
        };
        let a2 = Account {
            nonce: 0,
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
            incarnation: Incarnation(0),
        };
        let a3 = Account {
            nonce: 0,
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
            incarnation: Incarnation(0),
        };
        accounts.insert(
            H256::from(hex!(
                "00000000000000000000000000000000000000000000000000000028b2edaeaf"
            )),
            a1,
        );
        accounts.insert(
            H256::from(hex!(
                "7000000000000000000000000000000000000000000000000000000000000000"
            )),
            a2,
        );
        accounts.insert(
            H256::from(hex!(
                "7100000000000000000000000000000000000000000000000000000000000000"
            )),
            a3,
        );
        do_root_matches(accounts).await;
    }

    type Storage = BTreeMap<H256, H256>;

    fn account_storages() -> impl Strategy<Value = Storage> {
        prop::collection::btree_map(h256s(), h256s(), 0..100)
    }

    fn expected_storage_root(storage: Storage) -> H256 {
        if storage.is_empty() {
            EMPTY_ROOT
        } else {
            trie_root(
                storage
                    .iter()
                    .map(|(k, v)| (k.to_fixed_bytes(), rlp::encode(&zeroless_view(&v)))),
            )
        }
    }

    fn do_storage_root_matches(storage: Storage, hashed_address: H256, incarnation: Incarnation) {
        let mut _collector = Collector::<tables::TrieStorage>::new(OPTIMAL_BUFFER_CAPACITY);
        let vec_storage = storage.iter().map(|(&k, &v)| (k, v)).collect::<Vec<_>>();
        let actual = build_storage_trie(&mut _collector, hashed_address, incarnation, &vec_storage);
        let expected = expected_storage_root(storage);
        assert_eq!(expected, actual);
    }

    proptest! {
        #[test]
        fn storage_root_matches(
            storage in account_storages(),
            hashed_address in prop::array::uniform32(any::<u8>()),
            incarnation in 0u64..
        ) {
            do_storage_root_matches(storage, H256(hashed_address), Incarnation(incarnation));
        }
    }

    fn branch_data_sets() -> impl Strategy<Value = Vec<Option<Vec<u8>>>> {
        prop::collection::vec(prop::option::of(prop::collection::vec(any::<u8>(), 32)), 16)
    }

    proptest! {
        #[test]
        fn branch_node_roundtrip(data_set in branch_data_sets()) {
            let node = BranchNode::new_with_hashes(data_set);
            let serialized = node.serialize();
            let recovered = BranchNode::deserialize(&serialized).unwrap();
            assert_eq!(node, recovered);
        }
    }

    #[derive(Debug)]
    struct ChangingAccount {
        states: BTreeMap<u32, Account>,
    }

    impl ChangingAccount {
        // TODO handle case of accounts being created within the block range too
        fn new(states: BTreeMap<u32, Account>) -> Self {
            Self { states }
        }

        fn at(&self, block_number: u32) -> &Account {
            for (number, account) in self.states.iter().rev() {
                if number < &block_number {
                    return account;
                }
            }
            self.states.first_key_value().unwrap().1
            // all accounts are pre-existing, first block number is ignored
        }

        fn changed_between(&self, previous_block_number: u32, block_number: u32) -> Option<u32> {
            for height in self.states.keys().skip(1) {
                // again, first block number is treated as zero
                if *height > previous_block_number && *height <= block_number {
                    return Some(*height);
                }
            }
            None
        }
    }

    fn changing_accounts(block_range: &Range<u32>) -> impl Strategy<Value = ChangingAccount> {
        prop::collection::btree_map(block_range.clone(), accounts(), 1..=3)
            .prop_map(ChangingAccount::new)
    }

    fn sets_of_changing_accounts_accounts(
        max_accounts: usize,
        block_range: &Range<u32>,
    ) -> impl Strategy<Value = BTreeMap<Address, ChangingAccount>> {
        prop::collection::btree_map(
            addresses(),
            changing_accounts(block_range),
            1..=max_accounts,
        )
    }

    #[derive(Debug)]
    struct SetOfChangingAccounts {
        accounts: BTreeMap<Address, ChangingAccount>,
        block_range: Range<u32>,
    }

    fn range_u32() -> impl Strategy<Value = Range<u32>> {
        any::<[u32; 2]>().prop_map(|[a, b]| cmp::min(a, b)..cmp::max(a, b))
    }

    prop_compose! {
        fn sets_of_changing_accounts(max_accounts: usize)(
            block_range in range_u32()
        )(
            accounts in sets_of_changing_accounts_accounts(max_accounts, &block_range),
            block_range in Just(block_range)
        ) -> SetOfChangingAccounts {
            SetOfChangingAccounts { accounts, block_range }
        }
    }

    fn hashed_accounts_from_set_of_changing_accounts(
        set: &SetOfChangingAccounts,
        block_height: u32,
    ) -> BTreeMap<H256, Account> {
        let mut result = BTreeMap::new();
        for (address, changing_account) in &set.accounts {
            result.insert(
                keccak256(address),
                changing_account.at(block_height).clone(),
            );
        }
        result
    }

    async fn populate_hashed_accounts_from_set_of_changing_accounts<Db>(
        db: &Db,
        block_height: u32,
        set: &SetOfChangingAccounts,
    ) where
        Db: MutableKV,
    {
        let hash_to_account = hashed_accounts_from_set_of_changing_accounts(set, block_height);
        populate_hashed_accounts(db, &hash_to_account).await;
    }

    async fn populate_account_change_set<Db>(
        db: &Db,
        range: &Range<u32>,
        set: &SetOfChangingAccounts,
    ) where
        Db: MutableKV,
    {
        let mut changes = BTreeMap::new();
        for (address, changing_account) in &set.accounts {
            if let Some(height) = changing_account.changed_between(range.start, range.end) {
                changes.entry(height).or_insert_with(BTreeMap::new);
                let map = changes.get_mut(&height).unwrap();
                map.insert(address, changing_account.at(height).clone());
            }
        }

        let tx = db.begin_mutable().await.unwrap();

        {
            let mut cursor = tx
                .mutable_cursor_dupsort(&tables::AccountChangeSet)
                .await
                .unwrap();
            for (height, changes_at_height) in changes {
                for (address, account) in changes_at_height {
                    let account = account.encode_for_storage(false);
                    let entry = AccountChange {
                        address: *address,
                        account,
                    };
                    let fused_value = (BlockNumber(height as u64), entry);
                    cursor.append_dup(fused_value).await.unwrap();
                }
            }
        }

        tx.commit().await.unwrap();
    }

    async fn call_update_interhashes<Db>(db: &Db, from: u32, to: u32) -> H256
    where
        Db: MutableKV,
    {
        let mut tx = db.begin_mutable().await.unwrap();
        let mut collector = Collector::<tables::TrieAccount>::new(OPTIMAL_BUFFER_CAPACITY);
        let mut storage_collector = Collector::<tables::TrieStorage>::new(OPTIMAL_BUFFER_CAPACITY);

        update_interhashes2(
            &mut tx,
            &mut collector,
            &mut storage_collector,
            BlockNumber(from as u64),
            BlockNumber(to as u64),
        )
        .await
        .unwrap()
    }

    async fn do_updated_root_matches(changing_accounts: SetOfChangingAccounts) {
        let db = new_mem_database().unwrap();

        populate_hashed_accounts_from_set_of_changing_accounts(
            &db,
            changing_accounts.block_range.start,
            &changing_accounts,
        )
        .await;

        call_generate_interhashes(&db, true).await;
        populate_account_change_set(&db, &changing_accounts.block_range, &changing_accounts).await;

        populate_hashed_accounts_from_set_of_changing_accounts(
            &db,
            changing_accounts.block_range.end,
            &changing_accounts,
        )
        .await;

        let actual = call_update_interhashes(
            &db,
            changing_accounts.block_range.start,
            changing_accounts.block_range.end,
        )
        .await;
        let updated_hashed_accounts = hashed_accounts_from_set_of_changing_accounts(
            &changing_accounts,
            changing_accounts.block_range.end,
        );
        let expected = expected_root(&updated_hashed_accounts);

        assert_eq!(actual, expected);
    }

    proptest! {
        #[test]
        fn updated_root_matches(changing_accounts in sets_of_changing_accounts(500)) {
            tokio_test::block_on(do_updated_root_matches(changing_accounts));
        }
    }
}
