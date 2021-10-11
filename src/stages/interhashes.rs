use crate::{
    crypto::{keccak256, trie_root},
    etl::{
        collector::{Collector, OPTIMAL_BUFFER_CAPACITY},
        data_provider::Entry,
    },
    kv::{
        tables,
        traits::{Cursor, CursorDupSort, Table},
    },
    models::{Account, BlockNumber, Incarnation, RlpAccount, EMPTY_ROOT},
    stagedsync::stage::{ExecOutput, Stage, StageInput, UnwindInput},
    MutableTransaction, StageId,
};
use anyhow::Context;
use async_trait::async_trait;
use ethereum_types::H256;
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

fn to_nibbles(b: &[u8]) -> Vec<u8> {
    let mut n = vec![];
    for x in b {
        n.push(x / 0x10);
        n.push(x & 0x0f);
    }
    n
}

fn storage_seek_key(address_hash: &H256, incarnation: &Incarnation) -> Vec<u8> {
    let mut result = Vec::with_capacity(40);
    result.append(&mut address_hash.to_fixed_bytes().to_vec());
    result.append(&mut incarnation.to_be_bytes().to_vec());
    result
}

type HashedAccountFusedValue = <tables::HashedAccount as Table>::FusedValue;

#[derive(Debug)]
struct BranchInProgress {
    slot: u8,
    key_part: Vec<u8>,
    data: Vec<u8>,
    is_leaf: bool,
}

impl BranchInProgress {
    fn hash(&self) -> H256 {
        let encoded_key_part = hex_prefix(&self.key_part, self.is_leaf);
        let mut stream = RlpStream::new_list(2);
        stream.append(&encoded_key_part);
        stream.append(&self.data);
        keccak256(stream.out())
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

    fn get_single_branch(&self) -> &BranchInProgress {
        &self.branches[0]
    }

    fn get_root_branch(&mut self) -> &BranchInProgress {
        let branch = &mut self.branches[0];
        branch.key_part.insert(0, branch.slot);
        branch
    }
}

#[derive(Debug, PartialEq)]
struct BranchNode {
    hashes: Vec<Option<H256>>,
}

impl BranchNode {
    fn new() -> Self {
        Self {
            hashes: vec![None; 16],
        }
    }

    fn new_with_hashes(hashes: Vec<Option<H256>>) -> Self {
        Self { hashes }
    }

    fn from(node: &NodeInProgress) -> Self {
        let mut new_node = Self::new();
        for ref branch in &node.branches {
            let child_is_branch_node = branch.key_part.is_empty() && !branch.is_leaf;
            let hash = if child_is_branch_node {
                H256::from_slice(branch.data.as_slice())
            } else {
                branch.hash()
            };
            new_node.hashes[branch.slot as usize] = Some(hash);
        }
        new_node
    }

    fn serialize(&self) -> Vec<u8> {
        let mut flags = 0u16;
        let mut result = vec![0u8, 0u8];
        for (slot, ref maybe_hash) in self.hashes.iter().enumerate() {
            if let Some(hash) = maybe_hash {
                flags += 1u16.checked_shl(slot as u32).unwrap();
                result.append(&mut hash.as_ref().to_vec());
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
                node.hashes[i as usize] = Some(H256::from_slice(&data[index..index + 32]));
                index += 32;
            }
        }
        Ok(node)
    }

    fn hash(&self) -> H256 {
        let mut stream = RlpStream::new_list(17);
        for maybe_hash in &self.hashes {
            match maybe_hash {
                Some(ref hash) => stream.append(hash),
                None => stream.append_empty_data(),
            };
        }
        stream.append_empty_data();
        keccak256(stream.out())
    }
}

fn storage_root(storage: Vec<Vec<u8>>) -> anyhow::Result<H256> {
    Ok(if storage.is_empty() {
        EMPTY_ROOT
    } else {
        trie_root(storage.iter().map(|entry| (&entry[..32], &entry[32..])))
    })
}

struct GenerateWalker<'db: 'tx, 'tx: 'co, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    cursor: RwTx::Cursor<'tx, tables::HashedAccount>,
    storage_cursor: RwTx::CursorDupSort<'tx, tables::HashedStorage>,
    collector: &'co mut Collector<tables::TrieAccount>,
    in_progress: BTreeMap<Vec<u8>, NodeInProgress>,
    visited: Vec<u8>,
    last_prefix_length: usize,
}

impl<'db: 'tx, 'tx: 'co, 'co, RwTx> GenerateWalker<'db, 'tx, 'co, RwTx>
where
    RwTx: MutableTransaction<'db>,
{
    async fn new(
        tx: &'tx mut RwTx,
        collector: &'co mut Collector<tables::TrieAccount>,
    ) -> anyhow::Result<GenerateWalker<'db, 'tx, 'co, RwTx>>
    where
        RwTx: MutableTransaction<'db>,
    {
        let mut cursor = tx.cursor(&tables::HashedAccount).await?;
        let storage_cursor = tx.cursor_dup_sort(&tables::HashedStorage).await?;
        cursor.last().await?;
        let in_progress = Default::default();
        let visited = vec![];

        Ok(GenerateWalker {
            cursor,
            storage_cursor,
            collector,
            in_progress,
            visited,
            last_prefix_length: 63,
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
                .visit_storage(&address_hash, &account.incarnation)
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

    fn visit_account(&mut self, key: &Vec<u8>, account: &RlpAccount, prefix_length: usize) {
        let data = rlp::encode(account).to_vec();
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

    async fn storage_for_account(&mut self, key: Vec<u8>) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut storage = Vec::<Vec<u8>>::new();
        let mut found = self.storage_cursor.seek(key).await?;
        while let Some(entry) = found {
            storage.push(entry.1);
            found = self.storage_cursor.next_dup().await?;
        }
        Ok(storage)
    }

    async fn visit_storage(
        &mut self,
        address_hash: &H256,
        incarnation: &Incarnation,
    ) -> anyhow::Result<H256> {
        let key = storage_seek_key(address_hash, incarnation);
        let storage = self.storage_for_account(key).await?;
        Ok(storage_root(storage)?)
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

    fn finalize_branch_node(&mut self, key: &[u8], node: &NodeInProgress) -> H256 {
        let branch_node = BranchNode::from(node);
        let serialized = branch_node.serialize();
        self.collector.collect(Entry::new(key.to_vec(), serialized));
        branch_node.hash()
    }

    fn visit_node(&mut self, key: Vec<u8>, node: NodeInProgress) {
        let mut parent_key = key.clone();
        let slot = parent_key.pop().unwrap();
        if node.is_branch_node() {
            let hash = self.finalize_branch_node(&key, &node);
            let hash = hash.as_ref().to_vec();
            self.add_branch(parent_key, slot, vec![], hash, false);
        } else {
            let branch = node.get_single_branch();
            let mut key_part = branch.key_part.clone();
            key_part.insert(0, branch.slot);
            self.add_branch(
                parent_key,
                slot,
                key_part,
                branch.data.clone(),
                branch.is_leaf,
            );
        }
    }

    fn node_in_range(&mut self, prev_key: &Vec<u8>) -> Option<(Vec<u8>, NodeInProgress)> {
        if let Some(key) = self.in_progress.keys().last() {
            if key > prev_key {
                return self.in_progress.pop_last();
            }
        }
        None
    }

    fn prefix_length(&mut self, current_key: &[u8], prev_key: &[u8]) -> usize {
        if prev_key.is_empty() {
            return self.last_prefix_length;
        }
        let mut i = 0;
        while current_key[i] == prev_key[i] {
            i += 1;
        }
        let length = cmp::max(i, self.last_prefix_length);
        self.last_prefix_length = i;
        length
    }

    fn handle_range(&mut self, current_key: &Vec<u8>, account: &RlpAccount, prev_key: &Vec<u8>) {
        let prefix_length = self.prefix_length(current_key, prev_key);
        self.visit_account(&current_key, account, prefix_length);
        while let Some((key, node)) = self.node_in_range(&prev_key) {
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

async fn generate_interhashes<'db: 'tx, 'tx, RwTx>(
    tx: &mut RwTx,
    collector: &mut Collector<tables::TrieAccount>,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    let mut walker = GenerateWalker::new(tx, collector).await?;
    let mut current = walker.get_last_account().await?;

    while let Some(value) = current {
        let upcoming = walker.get_prev_account().await?;
        let current_key = to_nibbles(value.0.as_bytes());
        let upcoming_key = match upcoming {
            Some(ref v) => to_nibbles(v.0.as_bytes()),
            None => vec![],
        };
        walker.handle_range(&current_key, &value.1, &upcoming_key);
        current = upcoming;
    }

    Ok(walker.get_root())
}

async fn update_interhashes<'db: 'tx, 'tx, RwTx>(
    _tx: &mut RwTx,
    _collector: &mut Collector<tables::TrieAccount>,
    _from: BlockNumber,
    _to: BlockNumber,
) -> anyhow::Result<H256>
where
    RwTx: MutableTransaction<'db>,
{
    todo!()
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

            let _trie_root = if past_progress == BlockNumber(0) {
                generate_interhashes(tx, &mut collector)
                    .await
                    .with_context(|| "Failed to generate interhashes")?
            } else {
                update_interhashes(tx, &mut collector, past_progress, prev_progress)
                    .await
                    .with_context(|| "Failed to update interhashes")?
            };

            let mut write_cursor = tx.mutable_cursor(&tables::TrieAccount.erased()).await?;
            collector.load(&mut write_cursor).await?
        };

        info!("Processed");
        Ok(ExecOutput::Progress {
            stage_progress: cmp::max(prev_progress, past_progress),
            done: false,
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
        kv::traits::{MutableCursor, MutableKV, Transaction},
        models::EMPTY_HASH,
        new_mem_database,
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
            for (address_hash, account_model) in accounts {
                let account = Vec::from(account_model.encode_for_storage(false));
                let fused_value = (address_hash, account);
                cursor.append(fused_value).await.unwrap();
            }
        }

        tx.commit().await.unwrap();

        let mut tx = db.begin_mutable().await.unwrap();

        let mut _collector = Collector::<tables::TrieAccount>::new(OPTIMAL_BUFFER_CAPACITY);
        let root = generate_interhashes(&mut tx, &mut _collector)
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

    fn branch_hash_sets() -> impl Strategy<Value = Vec<Option<H256>>> {
        prop::collection::vec(prop::option::of(h256s()), 16)
    }

    proptest! {
        #[test]
        fn branch_node_roundtrip(hash_set in branch_hash_sets()) {
            let node = BranchNode::new_with_hashes(hash_set);
            let serialized = node.serialize();
            let recovered = BranchNode::deserialize(&serialized).unwrap();
            assert_eq!(node, recovered);
        }
    }
}