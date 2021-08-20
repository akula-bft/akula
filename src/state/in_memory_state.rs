use crate::{common::EMPTY_ROOT, crypto::sec_trie_root, models::*, State};
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_types::{Address, H256, U256};
use std::collections::HashMap;

// address -> initial value
type AccountChanges = HashMap<Address, Option<Account>>;

// address -> incarnation -> location -> initial value
type StorageChanges = HashMap<Address, HashMap<u64, HashMap<H256, H256>>>;

/// Holds all state in memory.
#[derive(Debug, Default)]
pub struct InMemoryState {
    accounts: HashMap<Address, Account>,

    // hash -> code
    code: HashMap<H256, Bytes<'static>>,
    prev_incarnations: HashMap<Address, u64>,

    // address -> incarnation -> location -> value
    storage: HashMap<Address, HashMap<u64, HashMap<H256, H256>>>,

    // block number -> hash -> header
    headers: Vec<HashMap<H256, BlockHeader>>,

    // block number -> hash -> body
    bodies: Vec<HashMap<H256, BlockBody>>,

    // block number -> hash -> total difficulty
    difficulty: Vec<HashMap<H256, U256>>,

    canonical_hashes: Vec<H256>,
    // per block
    account_changes: HashMap<u64, AccountChanges>,
    // per block
    storage_changes: HashMap<u64, StorageChanges>,

    block_number: u64,
}

impl InMemoryState {
    pub fn number_of_accounts(&self) -> usize {
        self.accounts.len()
    }

    pub fn storage_size(&self, address: Address, incarnation: u64) -> usize {
        if let Some(storage) = self.storage.get(&address) {
            if let Some(v) = storage.get(&incarnation) {
                return v.len();
            }
        }

        0
    }

    // https://eth.wiki/fundamentals/patricia-tree#storage-trie
    fn account_storage_root(&self, address: Address, incarnation: u64) -> H256 {
        if let Some(address_storage) = self.storage.get(&address) {
            if let Some(storage) = address_storage.get(&incarnation) {
                if !storage.is_empty() {
                    return sec_trie_root(storage);
                }
            }
        }

        EMPTY_ROOT
    }
}

#[async_trait]
impl State<'static> for InMemoryState {
    // Readers

    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        Ok(self.accounts.get(&address).cloned())
    }

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes<'static>> {
        Ok(self
            .code
            .get(&code_hash)
            .cloned()
            .unwrap_or_else(Bytes::new))
    }

    async fn read_storage(
        &self,
        address: Address,
        incarnation: u64,
        location: H256,
    ) -> anyhow::Result<H256> {
        if let Some(storage) = self.storage.get(&address) {
            if let Some(historical_data) = storage.get(&incarnation) {
                if let Some(value) = historical_data.get(&location) {
                    return Ok(*value);
                }
            }
        }

        Ok(H256::zero())
    }

    // Previous non-zero incarnation of an account; 0 if none exists.
    async fn previous_incarnation(&self, address: Address) -> anyhow::Result<u64> {
        Ok(self.prev_incarnations.get(&address).copied().unwrap_or(0))
    }

    async fn read_header(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        if let Some(header_map) = self.headers.get(block_number as usize) {
            return Ok(header_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    async fn read_body(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        if let Some(body_map) = self.bodies.get(block_number as usize) {
            return Ok(body_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    async fn read_body_with_senders(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        if let Some(body_map) = self.bodies.get(block_number as usize) {
            return body_map
                .get(&block_hash)
                .map(|body| {
                    Ok(BlockBodyWithSenders {
                        transactions: body
                            .transactions
                            .iter()
                            .map(|tx| {
                                Ok(TransactionWithSender::new(
                                    &tx.message,
                                    tx.recover_sender()?,
                                ))
                            })
                            .collect::<anyhow::Result<_>>()?,
                        ommers: body.ommers.clone(),
                    })
                })
                .transpose();
        }

        Ok(None)
    }

    async fn total_difficulty(
        &self,
        block_number: u64,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        if let Some(difficulty_map) = self.difficulty.get(block_number as usize) {
            return Ok(difficulty_map.get(&block_hash).cloned());
        }

        Ok(None)
    }

    async fn state_root_hash(&self) -> anyhow::Result<H256> {
        todo!()
    }

    async fn current_canonical_block(&self) -> anyhow::Result<u64> {
        Ok(self.canonical_hashes.len() as u64 - 1)
    }

    async fn canonical_hash(&self, block_number: u64) -> anyhow::Result<Option<H256>> {
        Ok(self.canonical_hashes.get(block_number as usize).copied())
    }

    async fn insert_block(&mut self, block: Block, hash: H256) -> anyhow::Result<()> {
        let Block {
            header,
            transactions,
            ommers,
        } = block;

        let block_number = header.number as usize;
        let parent_hash = header.parent_hash;
        let difficulty = header.difficulty;

        if self.headers.len() <= block_number {
            self.headers.resize_with(block_number + 1, Default::default);
        }
        self.headers[block_number].insert(hash, header);

        if self.bodies.len() <= block_number {
            self.bodies.resize_with(block_number + 1, Default::default);
        }
        self.bodies[block_number].insert(
            hash,
            BlockBody {
                transactions,
                ommers,
            },
        );

        if self.difficulty.len() <= block_number {
            self.difficulty
                .resize_with(block_number + 1, Default::default);
        }

        let d = {
            if block_number == 0 {
                U256::zero()
            } else {
                *self.difficulty[block_number - 1]
                    .entry(parent_hash)
                    .or_default()
            }
        } + difficulty;
        self.difficulty[block_number].entry(hash).insert(d);

        Ok(())
    }

    async fn canonize_block(&mut self, block_number: u64, block_hash: H256) -> anyhow::Result<()> {
        let block_number = block_number as usize;

        if self.canonical_hashes.len() <= block_number {
            self.canonical_hashes
                .resize_with(block_number + 1, Default::default);
        }

        self.canonical_hashes[block_number] = block_hash;

        Ok(())
    }

    async fn decanonize_block(&mut self, block_number: u64) -> anyhow::Result<()> {
        self.canonical_hashes.truncate(block_number as usize);

        Ok(())
    }

    async fn insert_receipts(&mut self, _: u64, _: &[Receipt]) -> anyhow::Result<()> {
        Ok(())
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: u64) {
        self.block_number = block_number;
        self.account_changes.remove(&block_number);
        self.storage_changes.remove(&block_number);
    }

    async fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) -> anyhow::Result<()> {
        self.account_changes
            .entry(self.block_number)
            .or_default()
            .insert(address, initial.clone());

        if let Some(current) = current {
            self.accounts.insert(address, current);
        } else {
            self.accounts.remove(&address);
            if let Some(initial) = initial {
                self.prev_incarnations.insert(address, initial.incarnation);
            }
        }

        Ok(())
    }

    async fn update_account_code(
        &mut self,
        _: Address,
        _: u64,
        code_hash: H256,
        code: Bytes<'static>,
    ) -> anyhow::Result<()> {
        // Don't overwrite already existing code so that views of it
        // that were previously returned by read_code() are still valid.
        self.code.entry(code_hash).or_insert(code);

        Ok(())
    }

    async fn update_storage(
        &mut self,
        address: Address,
        incarnation: u64,
        location: H256,
        initial: H256,
        current: H256,
    ) -> anyhow::Result<()> {
        self.storage_changes
            .entry(self.block_number)
            .or_default()
            .entry(address)
            .or_default()
            .entry(incarnation)
            .or_default()
            .insert(location, initial);

        let e = self
            .storage
            .entry(address)
            .or_default()
            .entry(incarnation)
            .or_default();

        if current.is_zero() {
            e.remove(&location);
        } else {
            e.insert(location, current);
        }

        Ok(())
    }

    async fn unwind_state_changes(&mut self, block_number: u64) -> anyhow::Result<()> {
        for (address, account) in self.account_changes.entry(block_number).or_default() {
            if let Some(account) = account {
                self.accounts.insert(*address, account.clone());
            } else {
                self.accounts.remove(address);
            }
        }

        for (address, storage1) in self.storage_changes.entry(block_number).or_default() {
            for (incarnation, storage2) in storage1 {
                for (location, value) in storage2 {
                    let e = self
                        .storage
                        .entry(*address)
                        .or_default()
                        .entry(*incarnation)
                        .or_default();
                    if value.is_zero() {
                        e.remove(location);
                    } else {
                        e.insert(*location, *value);
                    }
                }
            }
        }

        Ok(())
    }
}
