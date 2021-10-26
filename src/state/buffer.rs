use crate::{
    accessors,
    kv::{
        tables::{
            self, AccountChange, PlainStateFusedValue, PlainStateKey, StorageChange,
            StorageChangeKey,
        },
        traits::*,
    },
    models::{Receipt, *},
    state::database::*,
    MutableTransaction, State, Transaction,
};
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_types::{Address, H256, *};
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

    accounts: HashMap<Address, Option<Account>>,

    // address -> incarnation -> location -> value
    storage: HashMap<Address, BTreeMap<Incarnation, HashMap<H256, H256>>>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges>, // per block

    incarnations: BTreeMap<Address, Incarnation>,
    hash_to_code: BTreeMap<H256, Bytes>,
    storage_prefix_to_code_hash: BTreeMap<(Address, Incarnation), H256>,
    receipts: BTreeMap<BlockNumber, Vec<Receipt>>,
    logs: BTreeMap<(BlockNumber, TxIndex), Vec<Log>>,

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
            accounts: Default::default(),
            storage: Default::default(),
            account_changes: Default::default(),
            storage_changes: Default::default(),
            incarnations: Default::default(),
            hash_to_code: Default::default(),
            storage_prefix_to_code_hash: Default::default(),
            receipts: Default::default(),
            logs: Default::default(),
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
        if let Some(inc) = self.incarnations.get(&address).copied() {
            return Ok(inc);
        }

        Ok(
            accessors::state::read_previous_incarnation(self.txn, address, self.historical_block)
                .await?
                .unwrap_or_else(|| 0.into()),
        )
    }

    async fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        accessors::chain::header::read(self.txn, block_hash, block_number).await
    }

    async fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        accessors::chain::block_body::read_without_senders(self.txn, block_hash, block_number).await
    }

    async fn read_body_with_senders(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBodyWithSenders>> {
        accessors::chain::block_body::read_with_senders(self.txn, block_hash, block_number).await
    }

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        accessors::chain::td::read(self.txn, block_hash, block_number).await
    }

    async fn insert_receipts(
        &mut self,
        block_number: BlockNumber,
        receipts: Vec<Receipt>,
    ) -> anyhow::Result<()> {
        for (i, receipt) in receipts.iter().enumerate() {
            if !receipt.logs.is_empty() {
                self.logs
                    .insert((block_number, TxIndex(i as u64)), receipt.logs.clone());
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
        self.hash_to_code.insert(code_hash, code);
        self.storage_prefix_to_code_hash
            .insert((address, incarnation), code_hash);

        Ok(())
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
}

impl<'db, 'tx, Tx> Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    pub async fn write_to_db(self) -> anyhow::Result<()> {
        // Write to state table
        let mut state_table = self.txn.mutable_cursor_dupsort(&tables::PlainState).await?;

        let addresses = self.accounts.keys().chain(self.storage.keys());

        let mut storage_keys = Vec::new();

        for &address in addresses {
            if let Some(account) = self.accounts.get(&address) {
                if state_table
                    .seek_exact(PlainStateKey::Account(address))
                    .await?
                    .is_some()
                {
                    state_table.delete_current().await?;
                }

                if let Some(account) = account {
                    state_table
                        .upsert(PlainStateFusedValue::Account {
                            address,
                            account: account.encode_for_storage(false),
                        })
                        .await?;
                }
            }

            if let Some(storage) = self.storage.get(&address) {
                for (&incarnation, contract_storage) in storage {
                    storage_keys.clear();

                    for &x in contract_storage.keys() {
                        storage_keys.push(x);
                    }
                    storage_keys.sort_unstable();

                    for &k in &storage_keys {
                        upsert_storage_value(
                            &mut state_table,
                            address,
                            incarnation,
                            k,
                            contract_storage[&k],
                        )
                        .await?;
                    }
                }
            }
        }

        let mut incarnation_table = self.txn.mutable_cursor(&tables::IncarnationMap).await?;
        for fv in self.incarnations {
            incarnation_table.upsert(fv).await?;
        }

        let mut code_table = self.txn.mutable_cursor(&tables::Code).await?;
        for fv in self.hash_to_code {
            code_table.upsert(fv).await?;
        }

        let mut code_hash_table = self.txn.mutable_cursor(&tables::PlainCodeHash).await?;
        for fv in self.storage_prefix_to_code_hash {
            code_hash_table.upsert(fv).await?;
        }

        let mut account_change_table = self.txn.mutable_cursor(&tables::AccountChangeSet).await?;
        for (block_number, account_entries) in self.account_changes {
            for (address, account) in account_entries {
                account_change_table
                    .upsert((block_number, AccountChange { address, account }))
                    .await?;
            }
        }

        let mut storage_change_table = self.txn.mutable_cursor(&tables::StorageChangeSet).await?;
        for (block_number, storage_entries) in self.storage_changes {
            for (address, incarnation_entries) in storage_entries {
                for (incarnation, storage_entries) in incarnation_entries {
                    for (location, value) in storage_entries {
                        storage_change_table
                            .upsert((
                                StorageChangeKey {
                                    block_number,
                                    address,
                                    incarnation,
                                },
                                StorageChange { location, value },
                            ))
                            .await?;
                    }
                }
            }
        }

        let mut receipt_table = self.txn.mutable_cursor(&tables::Receipt).await?;
        for entry in self.receipts {
            receipt_table.upsert(entry).await?;
        }

        let mut log_table = self.txn.mutable_cursor(&tables::TransactionLog).await?;
        for entry in self.logs {
            log_table.upsert(entry).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::{tables::PlainStateFusedValue, traits::*},
        new_mem_database, DEFAULT_INCARNATION,
    };
    use hex_literal::hex;

    #[tokio::test]
    async fn storage_update() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address: Address = hex!("be00000000000000000000000000000000000000").into();

        let location_a =
            hex!("0000000000000000000000000000000000000000000000000000000000000013").into();
        let value_a1 =
            hex!("000000000000000000000000000000000000000000000000000000000000006b").into();
        let value_a2 =
            hex!("0000000000000000000000000000000000000000000000000000000000000085").into();

        let location_b =
            hex!("0000000000000000000000000000000000000000000000000000000000000002").into();
        let value_b =
            hex!("0000000000000000000000000000000000000000000000000000000000000132").into();

        txn.set(
            &tables::PlainState,
            PlainStateFusedValue::Storage {
                address,
                incarnation: DEFAULT_INCARNATION,
                location: location_a,
                value: value_a1,
            },
        )
        .await
        .unwrap();

        txn.set(
            &tables::PlainState,
            PlainStateFusedValue::Storage {
                address,
                incarnation: DEFAULT_INCARNATION,
                location: location_b,
                value: value_b,
            },
        )
        .await
        .unwrap();

        let mut buffer = Buffer::new(&txn, 0.into(), None);

        assert_eq!(
            buffer
                .read_storage(address, DEFAULT_INCARNATION, location_a)
                .await
                .unwrap(),
            value_a1
        );

        // Update only location A
        buffer
            .update_storage(address, DEFAULT_INCARNATION, location_a, value_a1, value_a2)
            .await
            .unwrap();
        buffer.write_to_db().await.unwrap();

        // Location A should have the new value
        let db_value_a = seek_storage_key(
            &mut txn.cursor_dup_sort(&tables::PlainState).await.unwrap(),
            address,
            DEFAULT_INCARNATION,
            location_a,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_a, value_a2);

        // Location B should not change
        let db_value_b = seek_storage_key(
            &mut txn.cursor_dup_sort(&tables::PlainState).await.unwrap(),
            address,
            DEFAULT_INCARNATION,
            location_b,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_b, value_b);
    }
}
