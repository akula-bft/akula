use crate::{
    accessors,
    kv::tables,
    models::{Receipt, *},
    MutableTransaction, State, Transaction,
};
use async_trait::async_trait;
use ethereum_types::{Address, H256, *};
use static_bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
};

// address -> storage-encoded initial value
pub type AccountChanges = BTreeMap<Address, EncodedAccount>;

// address -> incarnation -> location -> zeroless initial value
pub type StorageChanges = BTreeMap<Address, BTreeMap<Incarnation, BTreeMap<H256, H256>>>;

#[derive(Debug)]
pub struct Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: Transaction<'db>,
{
    txn: &'tx Tx,
    _marker: PhantomData<&'db ()>,

    prune_from: BlockNumber,
    historical_block: Option<BlockNumber>,

    headers: BTreeMap<(BlockNumber, H256), BlockHeader>,
    bodies: BTreeMap<(BlockNumber, H256), BlockBody>,
    difficulty: BTreeMap<(BlockNumber, H256), U256>,

    accounts: HashMap<Address, Option<Account>>,

    // address -> incarnation -> location -> value
    storage: HashMap<Address, BTreeMap<Incarnation, HashMap<H256, H256>>>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges>, // per block

    incarnations: BTreeMap<Address, Incarnation>,
    hash_to_code: BTreeMap<H256, Bytes>,
    storage_prefix_to_code_hash: BTreeMap<(Address, Incarnation), H256>,
    receipts: BTreeMap<BlockNumber, Vec<Receipt>>,
    logs: BTreeMap<(BlockNumber, u64), Vec<Log>>,

    batch_size: usize,

    // Current block stuff
    block_number: BlockNumber,
    changed_storage: HashSet<Address>,
}

impl<'db, 'tx, Tx> Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: Transaction<'db>,
{
    pub fn new(
        txn: &'tx Tx,
        prune_from: BlockNumber,
        historical_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            txn,
            prune_from,
            historical_block,
            _marker: PhantomData,
            headers: Default::default(),
            bodies: Default::default(),
            difficulty: Default::default(),
            accounts: Default::default(),
            storage: Default::default(),
            account_changes: Default::default(),
            storage_changes: Default::default(),
            incarnations: Default::default(),
            hash_to_code: Default::default(),
            storage_prefix_to_code_hash: Default::default(),
            receipts: Default::default(),
            logs: Default::default(),
            batch_size: Default::default(),
            block_number: Default::default(),
            changed_storage: Default::default(),
        }
    }
}

#[async_trait]
impl<'db, 'tx, Tx> State for Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: Transaction<'db>,
{
    async fn number_of_accounts(&self) -> anyhow::Result<u64> {
        todo!()
    }

    async fn storage_size(&self, _: Address, _: Incarnation) -> anyhow::Result<u64> {
        todo!()
    }

    // Readers

    async fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        if let Some(account) = self.accounts.get(&address) {
            return Ok(account.clone());
        }

        if let Some(enc) =
            crate::state::get_account_data_as_of(self.txn, address, self.block_number + 1).await?
        {
            return Account::decode_for_storage(&enc);
        }

        Ok(None)
    }

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        if let Some(code) = self.hash_to_code.get(&code_hash).cloned() {
            Ok(code)
        } else {
            Ok(self
                .txn
                .get(&tables::Code, code_hash)
                .await?
                .map(From::from)
                .unwrap_or_default())
        }
    }

    async fn read_storage(
        &self,
        address: Address,
        incarnation: Incarnation,
        location: H256,
    ) -> anyhow::Result<H256> {
        if let Some(it1) = self.storage.get(&address) {
            if let Some(it2) = it1.get(&incarnation) {
                if let Some(it3) = it2.get(&location) {
                    return Ok(*it3);
                }
            }
        }

        accessors::state::storage::read(
            self.txn,
            address,
            incarnation,
            location,
            self.historical_block,
        )
        .await
    }

    // Previous non-zero incarnation of an account; 0 if none exists.
    async fn previous_incarnation(&self, address: Address) -> anyhow::Result<Incarnation> {
        todo!()
    }

    async fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        todo!()
    }

    async fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        todo!()
    }

    async fn read_body_with_senders(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        todo!()
    }

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        todo!()
    }

    async fn state_root_hash(&self) -> anyhow::Result<H256> {
        todo!()
    }

    async fn current_canonical_block(&self) -> anyhow::Result<BlockNumber> {
        todo!()
    }

    async fn canonical_hash(&self, block_number: BlockNumber) -> anyhow::Result<Option<H256>> {
        todo!()
    }

    async fn insert_block(&mut self, block: Block, hash: H256) -> anyhow::Result<()> {
        todo!()
    }

    async fn canonize_block(
        &mut self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn decanonize_block(&mut self, block_number: BlockNumber) -> anyhow::Result<()> {
        todo!()
    }

    async fn insert_receipts(
        &mut self,
        block_number: BlockNumber,
        receipts: Vec<Receipt>,
    ) -> anyhow::Result<()> {
        for (i, receipt) in receipts.iter().enumerate() {
            if !receipt.logs.is_empty() {
                self.logs
                    .insert((block_number, i as u64), receipt.logs.clone());
            }
        }

        self.receipts.insert(block_number, receipts);

        Ok(())
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber) {
        self.block_number = block_number;
        self.changed_storage.clear();
    }

    async fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) -> anyhow::Result<()> {
        let equal = current == initial;
        let account_deleted = current.is_none();

        if equal && !account_deleted && !self.changed_storage.contains(&address) {
            return Ok(());
        }

        if self.block_number >= self.prune_from {
            let mut encoded_initial = EncodedAccount::default();
            if let Some(initial) = &initial {
                let omit_code_hash = !account_deleted;
                encoded_initial = initial.encode_for_storage(omit_code_hash);
            }

            self.account_changes
                .entry(self.block_number)
                .or_default()
                .insert(address, encoded_initial);
        }

        if equal {
            return Ok(());
        }

        self.accounts.insert(address, current);

        if account_deleted {
            let initial = initial.expect("deleted account must have existed before");
            if initial.incarnation.0 > 0 {
                self.incarnations.insert(address, initial.incarnation);
            }
        }

        Ok(())
    }

    async fn update_account_code(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        code_hash: H256,
        code: Bytes,
    ) -> anyhow::Result<()> {
        todo!()
    }

    async fn update_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        location: H256,
        initial: H256,
        current: H256,
    ) -> anyhow::Result<()> {
        if current == initial {
            return Ok(());
        }

        if self.block_number >= self.prune_from {
            self.changed_storage.insert(address);
            self.storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default()
                .entry(incarnation)
                .or_default()
                .insert(location, initial);
        }

        self.storage
            .entry(address)
            .or_default()
            .entry(incarnation)
            .or_default()
            .insert(location, current);

        Ok(())
    }

    async fn unwind_state_changes(&mut self, block_number: BlockNumber) -> anyhow::Result<()> {
        todo!()
    }
}

impl<'db, 'tx, Tx> Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    pub async fn write_to_db(self) -> anyhow::Result<()> {
        todo!()
    }
}
