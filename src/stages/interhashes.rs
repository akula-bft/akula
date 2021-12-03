use crate::{
    accessors,
    crypto::keccak256,
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::{
        tables,
        traits::{Cursor, CursorDupSort},
        TableEncode,
    },
    models::{Account, BlockNumber, RlpAccount, EMPTY_ROOT},
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput, *},
    stages::stage_util::should_do_clean_promotion,
    MutableTransaction, StageId,
};
use anyhow::{bail, format_err, Context};
use async_trait::async_trait;
use ethereum_types::*;
use rlp::RlpStream;
use std::{cmp, collections::BTreeMap};
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
    fn new(collector: &'co mut Collector<tables::TrieStorage>, hashed_address: H256) -> Self {
        Self {
            collector,
            path_prefix: TableEncode::encode(hashed_address).to_vec(),
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
    storage: &[(H256, U256)],
) -> H256 {
    if storage.is_empty() {
        return EMPTY_ROOT;
    }

    let wrapped_collector = StorageTrieCollector::new(collector, address_hash);
    let mut builder = TrieBuilder::new(wrapped_collector);

    let mut storage_iter = storage.iter().rev();
    let mut current = storage_iter.next();

    while let Some(&(location, value)) = &mut current {
        let current_value = value.encode();
        let current_key = location.into();
        let prev = storage_iter.next();

        let prev_key = prev.map(|&(v, _)| v.into());

        let data = rlp::encode(&current_value.as_slice()).to_vec();

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
        tx: &'tx RwTx,
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
        value: Option<(H256, Account)>,
    ) -> anyhow::Result<Option<(H256, RlpAccount)>> {
        if let Some((address_hash, account)) = value {
            let storage_root = self.visit_storage(address_hash).await?;
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
    ) -> anyhow::Result<Vec<(H256, U256)>> {
        let mut storage = Vec::<(H256, U256)>::new();
        let mut found = self.storage_cursor.seek_exact(address_hash).await?;
        while let Some((_, storage_entry)) = found {
            storage.push((storage_entry.0, storage_entry.1));
            found = self.storage_cursor.next_dup().await?;
        }
        Ok(storage)
    }

    async fn visit_storage(&mut self, address_hash: H256) -> anyhow::Result<H256> {
        let storage = self.storage_for_account(address_hash).await?;
        let storage_root = build_storage_trie(self.storage_collector, address_hash, &storage);
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

    fn get_root(&mut self) -> H256 {
        self.trie_builder.get_root()
    }
}

pub async fn generate_interhashes<'db: 'tx, 'tx, RwTx>(tx: &RwTx) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
    let mut storage_collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

    let state_root =
        generate_interhashes_with_collectors(tx, &mut collector, &mut storage_collector).await?;

    let mut write_cursor = tx.mutable_cursor(&tables::TrieAccount.erased()).await?;
    collector.load(&mut write_cursor).await?;
    let mut storage_write_cursor = tx.mutable_cursor(&tables::TrieStorage.erased()).await?;
    storage_collector.load(&mut storage_write_cursor).await?;

    Ok(state_root)
}

async fn generate_interhashes_with_collectors<'db: 'tx, 'tx, RwTx>(
    tx: &RwTx,
    collector: &mut Collector<tables::TrieAccount>,
    storage_collector: &mut Collector<tables::TrieStorage>,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    tx.clear_table(&tables::TrieAccount).await?;
    tx.clear_table(&tables::TrieStorage).await?;

    let mut walker = GenerateWalker::new(tx, collector, storage_collector).await?;
    let mut current = walker.get_last_account().await?;

    while let Some((hashed_account_key, account_data)) = current {
        let upcoming = walker.get_prev_account().await?;
        let current_key = hashed_account_key.into();
        let upcoming_key = upcoming.as_ref().map(|&(key, _)| key.into());
        walker.handle_range(current_key, &account_data, upcoming_key);
        current = upcoming;
    }

    Ok(walker.get_root())
}

async fn update_interhashes<'db: 'tx, 'tx, RwTx>(
    tx: &RwTx,
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

    generate_interhashes_with_collectors(tx, collector, storage_collector).await
}

#[derive(Debug)]
pub struct Interhashes {
    clean_promotion_threshold: u64,
}

impl Interhashes {
    pub fn new(clean_promotion_threshold: Option<u64>) -> Self {
        Self {
            clean_promotion_threshold: clean_promotion_threshold.unwrap_or(1_000_000_000_000),
        }
    }
}

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
        let genesis = BlockNumber(0);
        let max_block = input
            .previous_stage
            .map(|tuple| tuple.1)
            .ok_or_else(|| format_err!("Cannot be first stage"))?;
        let past_progress = input.stage_progress.unwrap_or(genesis);

        if max_block > past_progress {
            let mut collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);
            let mut storage_collector = Collector::new(OPTIMAL_BUFFER_CAPACITY);

            let trie_root = if should_do_clean_promotion(
                tx,
                genesis,
                past_progress,
                max_block,
                self.clean_promotion_threshold,
            )
            .await?
            {
                generate_interhashes_with_collectors(tx, &mut collector, &mut storage_collector)
                    .await
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                update_interhashes(
                    tx,
                    &mut collector,
                    &mut storage_collector,
                    past_progress,
                    max_block,
                )
                .await
                .with_context(|| "Failed to update interhashes")?
            };

            let block_state_root = accessors::chain::header::read(
                tx,
                accessors::chain::canonical_hash::read(tx, max_block)
                    .await?
                    .ok_or_else(|| format_err!("No canonical hash for block {}", max_block))?,
                max_block,
            )
            .await?
            .ok_or_else(|| format_err!("No header for block {}", max_block))?
            .state_root;

            if block_state_root == trie_root {
                info!("Block #{} state root OK: {:?}", max_block, trie_root)
            } else {
                bail!(
                    "Block #{} state root mismatch: {:?} != {:?}",
                    max_block,
                    trie_root,
                    block_state_root
                )
            }

            let mut write_cursor = tx.mutable_cursor(&tables::TrieAccount.erased()).await?;
            collector.load(&mut write_cursor).await?;
            let mut storage_write_cursor = tx.mutable_cursor(&tables::TrieStorage.erased()).await?;
            storage_collector.load(&mut storage_write_cursor).await?
        };

        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(max_block, past_progress),
            done: true,
            must_commit: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let _ = input;
        // TODO: proper unwind
        tx.clear_table(&tables::TrieAccount).await?;
        tx.clear_table(&tables::TrieStorage).await?;

        Ok(UnwindOutput {
            stage_progress: BlockNumber(0),
            must_commit: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        crypto::trie_root,
        h256_to_u256,
        kv::traits::{MutableCursor, MutableKV},
        models::EMPTY_HASH,
        new_mem_database, u256_to_h256, zeroless_view,
    };
    use ethereum_types::{H256, U256};
    use hex_literal::hex;
    use proptest::prelude::*;

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

    fn h256s() -> impl Strategy<Value = H256> {
        any::<[u8; 32]>().prop_map(H256::from)
    }

    fn u256s() -> impl Strategy<Value = U256> {
        any::<[u8; 32]>().prop_map(|v| h256_to_u256(H256::from(v)))
    }

    prop_compose! {
        fn accounts()(
            nonce in any::<u64>(),
            balance in any::<[u8; 32]>(),
            code_hash in any::<[u8; 32]>(),
        ) -> Account {
            let balance = U256::from(balance);
            let code_hash = H256::from(code_hash);
            Account { nonce, balance, code_hash }
        }
    }

    fn maps_of_accounts() -> impl Strategy<Value = BTreeMap<H256, Account>> {
        prop::collection::btree_map(h256s(), accounts(), 1..500)
    }

    fn expected_root(accounts: &BTreeMap<H256, Account>) -> H256 {
        trie_root(accounts.iter().map(|(&address_hash, account)| {
            let account_rlp = account.to_rlp(EMPTY_ROOT);
            (address_hash, rlp::encode(&account_rlp))
        }))
    }

    async fn do_root_matches(accounts: BTreeMap<H256, Account>) {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();
        let expected = expected_root(&accounts);

        {
            let mut cursor = tx.mutable_cursor(&tables::HashedAccount).await.unwrap();
            for (address_hash, account) in accounts {
                cursor.append(address_hash, account).await.unwrap();
            }
        }

        tx.commit().await.unwrap();

        let tx = db.begin_mutable().await.unwrap();

        let mut _collector = Collector::<tables::TrieAccount>::new(OPTIMAL_BUFFER_CAPACITY);
        let mut _storage_collector = Collector::<tables::TrieStorage>::new(OPTIMAL_BUFFER_CAPACITY);
        let root =
            generate_interhashes_with_collectors(&tx, &mut _collector, &mut _storage_collector)
                .await
                .unwrap();
        assert_eq!(root, expected);
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
        };
        let a2 = Account {
            nonce: 0,
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
        };
        let a3 = Account {
            nonce: 0,
            balance: U256::zero(),
            code_hash: EMPTY_HASH,
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

    type Storage = BTreeMap<H256, U256>;

    fn account_storages() -> impl Strategy<Value = Storage> {
        prop::collection::btree_map(h256s(), u256s(), 0..100)
    }

    fn expected_storage_root(storage: Storage) -> H256 {
        if storage.is_empty() {
            EMPTY_ROOT
        } else {
            trie_root(storage.iter().map(|(k, v)| {
                (
                    k.to_fixed_bytes(),
                    rlp::encode(&zeroless_view(&u256_to_h256(*v))),
                )
            }))
        }
    }

    fn do_storage_root_matches(storage: Storage, hashed_address: H256) {
        let mut _collector = Collector::<tables::TrieStorage>::new(OPTIMAL_BUFFER_CAPACITY);
        let vec_storage = storage.iter().map(|(&k, &v)| (k, v)).collect::<Vec<_>>();
        let actual = build_storage_trie(&mut _collector, hashed_address, &vec_storage);
        let expected = expected_storage_root(storage);
        assert_eq!(expected, actual);
    }

    proptest! {
        #[test]
        fn storage_root_matches(
            storage in account_storages(),
            hashed_address in prop::array::uniform32(any::<u8>()),
        ) {
            do_storage_root_matches(storage, H256(hashed_address));
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
}
