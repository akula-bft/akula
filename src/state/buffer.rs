use crate::{
    accessors, h256_to_u256,
    kv::{
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
        traits::*,
    },
    models::*,
    state::database::*,
    u256_to_h256, State,
};
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    marker::PhantomData,
};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;

// address -> storage-encoded initial value
pub type AccountChanges = BTreeMap<Address, Option<Account>>;

// address -> location -> zeroless initial value
pub type StorageChanges = BTreeMap<Address, BTreeMap<U256, U256>>;

#[derive(Default, Debug)]
struct OverlayStorage {
    erased: bool,
    slots: HashMap<U256, U256>,
}

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

    // address -> location -> value
    storage: HashMap<Address, OverlayStorage>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges>, // per block

    hash_to_code: BTreeMap<H256, Bytes>,
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
            hash_to_code: Default::default(),
            logs: Default::default(),
            block_number: Default::default(),
            changed_storage: Default::default(),
        }
    }

    pub fn insert_receipts(&mut self, block_number: BlockNumber, receipts: Vec<Receipt>) {
        for (i, receipt) in receipts.into_iter().enumerate() {
            self.logs
                .insert((block_number, TxIndex(i.try_into().unwrap())), receipt.logs);
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
            return Ok(*account);
        }

        accessors::state::account::read(self.txn, address, self.historical_block).await
    }

    async fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        if let Some(code) = self.hash_to_code.get(&code_hash).cloned() {
            Ok(code)
        } else {
            Ok(self
                .txn
                .get(tables::Code, code_hash)
                .await?
                .map(From::from)
                .unwrap_or_default())
        }
    }

    async fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256> {
        if let Some(account_storage) = self.storage.get(&address) {
            if let Some(value) = account_storage.slots.get(&location) {
                return Ok(*value);
            } else if account_storage.erased {
                return Ok(U256::ZERO);
            }
        }

        accessors::state::storage::read(self.txn, address, location, self.historical_block).await
    }

    async fn erase_storage(&mut self, address: Address) -> anyhow::Result<()> {
        let mut mark_database_as_discarded = false;
        let overlay_storage = self.storage.entry(address).or_insert_with(|| {
            // If we don't have any overlay storage, we must mark slots in database as zeroed.
            mark_database_as_discarded = true;

            OverlayStorage {
                erased: true,
                slots: Default::default(),
            }
        });

        let storage_changes = self
            .storage_changes
            .entry(self.block_number)
            .or_default()
            .entry(address)
            .or_default();

        for (slot, value) in overlay_storage.slots.drain() {
            storage_changes.insert(slot, value);
        }

        if !overlay_storage.erased {
            // If we haven't erased before, we also must mark unmodified slots as zeroed.
            mark_database_as_discarded = true;
            overlay_storage.erased = true;
        }

        if mark_database_as_discarded {
            let mut storage_table = self.txn.cursor_dup_sort(tables::Storage).await?;

            let walker = walk(&mut storage_table, Some(address));
            pin!(walker);

            let storage_changes = self
                .storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default();

            while let Some((a, (slot, initial))) = walker.try_next().await? {
                if a != address {
                    break;
                }

                // Only insert slot from db if it's not in storage buffer yet.
                storage_changes.entry(h256_to_u256(slot)).or_insert(initial);
            }
        }

        Ok(())
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

    async fn total_difficulty(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<U256>> {
        accessors::chain::td::read(self.txn, block_hash, block_number).await
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber) {
        self.block_number = block_number;
        self.changed_storage.clear();
    }

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) {
        let equal = current == initial;
        let account_deleted = current.is_none();

        if equal && !account_deleted && !self.changed_storage.contains(&address) {
            return;
        }

        if self.block_number >= self.prune_from {
            self.account_changes
                .entry(self.block_number)
                .or_default()
                .insert(address, initial);
        }

        if equal {
            return;
        }

        self.accounts.insert(address, current);
    }

    async fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()> {
        self.hash_to_code.insert(code_hash, code);

        Ok(())
    }

    async fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
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
                .insert(location, initial);
        }

        self.storage
            .entry(address)
            .or_default()
            .slots
            .insert(location, current);

        Ok(())
    }
}

impl<'db, 'tx, Tx> Buffer<'db, 'tx, Tx>
where
    'db: 'tx,
    Tx: MutableTransaction<'db>,
{
    pub async fn write_history(&mut self) -> anyhow::Result<()> {
        debug!("Writing account changes");
        let mut account_change_table = self
            .txn
            .mutable_cursor_dupsort(tables::AccountChangeSet)
            .await?;
        for (block_number, account_entries) in std::mem::take(&mut self.account_changes) {
            for (address, account) in account_entries {
                account_change_table
                    .append_dup(block_number, AccountChange { address, account })
                    .await?;
            }
        }

        debug!("Writing storage changes");
        let mut storage_change_table = self
            .txn
            .mutable_cursor_dupsort(tables::StorageChangeSet)
            .await?;
        for (block_number, storage_entries) in std::mem::take(&mut self.storage_changes) {
            for (address, storage_entries) in storage_entries {
                for (location, value) in storage_entries {
                    let location = u256_to_h256(location);
                    storage_change_table
                        .append_dup(
                            StorageChangeKey {
                                block_number,
                                address,
                            },
                            StorageChange { location, value },
                        )
                        .await?;
                }
            }
        }

        debug!("Writing logs");
        let mut log_table = self.txn.mutable_cursor(tables::Log).await?;
        for ((block_number, idx), logs) in std::mem::take(&mut self.logs) {
            log_table.append((block_number, idx), logs).await?;
        }

        debug!("History write complete");

        Ok(())
    }

    pub async fn write_to_db(mut self) -> anyhow::Result<()> {
        self.write_history().await?;

        // Write to state tables
        let mut account_table = self.txn.mutable_cursor(tables::Account).await?;
        let mut storage_table = self.txn.mutable_cursor_dupsort(tables::Storage).await?;

        debug!("Writing accounts");
        let mut account_addresses = self.accounts.keys().collect::<Vec<_>>();
        account_addresses.sort_unstable();
        let mut written_accounts = 0;
        for &address in account_addresses {
            let account = self.accounts[&address];

            if let Some(account) = account {
                account_table.upsert(address, account).await?;
            } else if account_table.seek_exact(address).await?.is_some() {
                account_table.delete_current().await?;
            }

            written_accounts += 1;
            if written_accounts % 500_000 == 0 {
                debug!(
                    "Written {} updated acccounts, current entry: {}",
                    written_accounts, address
                )
            }
        }

        debug!("Writing {} accounts complete", written_accounts);

        debug!("Writing storage");
        let mut storage_addresses = self.storage.keys().collect::<Vec<_>>();
        storage_addresses.sort_unstable();
        let mut written_slots = 0;
        for &address in storage_addresses {
            let overlay_storage = &self.storage[&address];

            if overlay_storage.erased && storage_table.seek_exact(address).await?.is_some() {
                storage_table.delete_current_duplicates().await?;
            }

            for (&k, &v) in &overlay_storage.slots {
                upsert_storage_value(&mut storage_table, address, k, v).await?;

                written_slots += 1;
                if written_slots % 500_000 == 0 {
                    debug!(
                        "Written {} storage slots, current entry: address {}, slot {}",
                        written_slots, address, k
                    );
                }
            }
        }

        debug!("Writing {} slots complete", written_slots);

        debug!("Writing code");
        let mut code_table = self.txn.mutable_cursor(tables::Code).await?;
        for (code_hash, code) in self.hash_to_code {
            code_table.upsert(code_hash, code).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{h256_to_u256, kv::new_mem_database};
    use hex_literal::hex;

    #[tokio::test]
    async fn storage_update() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address: Address = hex!("be00000000000000000000000000000000000000").into();

        let location_a = H256(hex!(
            "0000000000000000000000000000000000000000000000000000000000000013"
        ));
        let value_a1 = 0x6b.as_u256();
        let value_a2 = 0x85.as_u256();

        let location_b = H256(hex!(
            "0000000000000000000000000000000000000000000000000000000000000002"
        ));
        let value_b = 0x132.as_u256();

        txn.set(tables::Storage, address, (location_a, value_a1))
            .await
            .unwrap();

        txn.set(tables::Storage, address, (location_b, value_b))
            .await
            .unwrap();

        let mut buffer = Buffer::new(&txn, 0.into(), None);

        assert_eq!(
            buffer
                .read_storage(address, h256_to_u256(location_a))
                .await
                .unwrap(),
            value_a1
        );

        // Update only location A
        buffer
            .update_storage(address, h256_to_u256(location_a), value_a1, value_a2)
            .await
            .unwrap();
        buffer.write_to_db().await.unwrap();

        // Location A should have the new value
        let db_value_a = seek_storage_key(
            &mut txn.cursor_dup_sort(tables::Storage).await.unwrap(),
            address,
            h256_to_u256(location_a),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_a, value_a2);

        // Location B should not change
        let db_value_b = seek_storage_key(
            &mut txn.cursor_dup_sort(tables::Storage).await.unwrap(),
            address,
            h256_to_u256(location_b),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_b, value_b);
    }
}
