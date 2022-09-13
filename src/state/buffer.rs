use crate::{
    accessors, h256_to_u256,
    kv::{
        mdbx::*,
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
    },
    models::*,
    state::database::*,
    u256_to_h256, BlockReader, HeaderReader, StateReader, StateWriter,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use tokio::pin;
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
pub struct Buffer<'db, 'tx, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    txn: &'tx MdbxTransaction<'db, K, E>,

    historical_block: Option<BlockNumber>,

    accounts: HashMap<Address, Option<Account>>,

    // address -> location -> value
    storage: HashMap<Address, OverlayStorage>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges>, // per block

    hash_to_code: BTreeMap<H256, Bytes>,
    log_index: BTreeMap<BlockNumber, (BTreeSet<Address>, BTreeSet<H256>)>,

    // Current block stuff
    block_number: BlockNumber,
}

impl<'db, 'tx, K, E> Buffer<'db, 'tx, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    pub fn new(
        txn: &'tx MdbxTransaction<'db, K, E>,
        historical_block: Option<BlockNumber>,
    ) -> Self {
        Self {
            txn,
            historical_block,
            accounts: Default::default(),
            storage: Default::default(),
            account_changes: Default::default(),
            storage_changes: Default::default(),
            hash_to_code: Default::default(),
            log_index: Default::default(),
            block_number: Default::default(),
        }
    }

    pub fn insert_receipts(&mut self, block_number: BlockNumber, receipts: Vec<Receipt>) {
        self.log_index.insert(
            block_number,
            receipts.into_iter().fold(
                Default::default(),
                |(mut addresses, mut topics), receipt| {
                    for log in receipt.logs {
                        addresses.insert(log.address);
                        for topic in log.topics {
                            topics.insert(topic);
                        }
                    }

                    (addresses, topics)
                },
            ),
        );
    }
}

impl<'db, 'tx, K, E> HeaderReader for MdbxTransaction<'db, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        self.get(tables::Header, (block_number, block_hash))
    }
}

impl<'db, 'tx, K, E> BlockReader for MdbxTransaction<'db, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_body(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockBody>> {
        accessors::chain::block_body::read_without_senders(self, block_hash, block_number)
    }
}

impl<'db, 'tx, K, E> HeaderReader for Buffer<'db, 'tx, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_header(
        &self,
        block_number: BlockNumber,
        block_hash: H256,
    ) -> anyhow::Result<Option<BlockHeader>> {
        self.txn.read_header(block_number, block_hash)
    }
}

impl<'db, 'tx, K, E> StateReader for Buffer<'db, 'tx, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn read_account(&self, address: Address) -> anyhow::Result<Option<Account>> {
        if let Some(account) = self.accounts.get(&address) {
            return Ok(*account);
        }

        accessors::state::account::read(self.txn, address, self.historical_block)
    }

    fn read_code(&self, code_hash: H256) -> anyhow::Result<Bytes> {
        if let Some(code) = self.hash_to_code.get(&code_hash).cloned() {
            Ok(code)
        } else {
            Ok(self
                .txn
                .get(tables::Code, code_hash)?
                .map(From::from)
                .unwrap_or_default())
        }
    }

    fn read_storage(&self, address: Address, location: U256) -> anyhow::Result<U256> {
        if let Some(account_storage) = self.storage.get(&address) {
            if let Some(value) = account_storage.slots.get(&location) {
                return Ok(*value);
            } else if account_storage.erased {
                return Ok(U256::ZERO);
            }
        }

        accessors::state::storage::read(self.txn, address, location, self.historical_block)
    }
}

impl<'db, 'tx, K, E> StateWriter for Buffer<'db, 'tx, K, E>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    fn erase_storage(&mut self, address: Address) -> anyhow::Result<()> {
        let mut mark_database_as_discarded = false;
        let overlay_storage = self.storage.entry(address).or_insert_with(|| {
            // If we don't have any overlay storage, we must mark slots in database as zeroed.
            mark_database_as_discarded = true;

            OverlayStorage {
                erased: true,
                slots: Default::default(),
            }
        });

        if !overlay_storage.slots.is_empty() {
            let storage_changes = self
                .storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default();

            for (slot, value) in overlay_storage.slots.drain() {
                storage_changes.insert(slot, value);
            }
        }

        if !overlay_storage.erased {
            // If we haven't erased before, we also must mark unmodified slots as zeroed.
            mark_database_as_discarded = true;
            overlay_storage.erased = true;
        }

        if mark_database_as_discarded {
            let storage_table = self.txn.cursor(tables::Storage)?;

            let walker = storage_table.walk_dup(address, None);
            pin!(walker);

            let storage_changes = self
                .storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default();

            while let Some((slot, initial)) = walker.next().transpose()? {
                // Only insert slot from db if it's not in storage buffer yet.
                storage_changes.entry(h256_to_u256(slot)).or_insert(initial);
            }
        }

        Ok(())
    }

    /// State changes
    /// Change sets are backward changes of the state, i.e. account/storage values _at the beginning of a block_.

    /// Mark the beggining of a new block.
    /// Must be called prior to calling update_account/update_account_code/update_storage.
    fn begin_block(&mut self, block_number: BlockNumber) {
        self.block_number = block_number;
    }

    fn update_account(
        &mut self,
        address: Address,
        initial: Option<Account>,
        current: Option<Account>,
    ) {
        if initial != current {
            self.account_changes
                .entry(self.block_number)
                .or_default()
                .insert(address, initial);

            self.accounts.insert(address, current);
        }
    }

    fn update_code(&mut self, code_hash: H256, code: Bytes) -> anyhow::Result<()> {
        self.hash_to_code.insert(code_hash, code);

        Ok(())
    }

    fn update_storage(
        &mut self,
        address: Address,
        location: U256,
        initial: U256,
        current: U256,
    ) -> anyhow::Result<()> {
        if initial != current {
            self.storage_changes
                .entry(self.block_number)
                .or_default()
                .entry(address)
                .or_default()
                .insert(location, initial);

            self.storage
                .entry(address)
                .or_default()
                .slots
                .insert(location, current);
        }

        Ok(())
    }
}

impl<'db, 'tx, E> Buffer<'db, 'tx, RW, E>
where
    'db: 'tx,
    E: EnvironmentKind,
{
    pub fn write_history(&mut self) -> anyhow::Result<()> {
        debug!("Writing account changes");
        let mut account_change_table = self.txn.cursor(tables::AccountChangeSet)?;
        for (block_number, account_entries) in std::mem::take(&mut self.account_changes) {
            for (address, account) in account_entries {
                account_change_table
                    .append_dup(block_number, AccountChange { address, account })?;
            }
        }

        debug!("Writing storage changes");
        let mut storage_change_table = self.txn.cursor(tables::StorageChangeSet)?;
        for (block_number, storage_entries) in std::mem::take(&mut self.storage_changes) {
            for (address, storage_entries) in storage_entries {
                for (location, value) in storage_entries {
                    let location = u256_to_h256(location);
                    storage_change_table.append_dup(
                        StorageChangeKey {
                            block_number,
                            address,
                        },
                        StorageChange { location, value },
                    )?;
                }
            }
        }

        debug!("Writing logs");
        let mut log_addresses = self.txn.cursor(tables::LogAddressesByBlock)?;
        let mut log_topics = self.txn.cursor(tables::LogTopicsByBlock)?;
        for (block_number, (addresses, topics)) in std::mem::take(&mut self.log_index) {
            for address in addresses {
                log_addresses.append_dup(block_number, address)?;
            }
            for topic in topics {
                log_topics.append_dup(block_number, topic)?;
            }
        }

        debug!("History write complete");

        Ok(())
    }

    pub fn write_to_db(mut self) -> anyhow::Result<()> {
        self.write_history()?;

        // Write to state tables
        let mut account_table = self.txn.cursor(tables::Account)?;
        let mut storage_table = self.txn.cursor(tables::Storage)?;

        debug!("Writing accounts");
        let mut account_addresses = self.accounts.keys().collect::<Vec<_>>();
        account_addresses.sort_unstable();
        let mut written_accounts = 0;
        for &address in account_addresses {
            let account = self.accounts[&address];

            if let Some(account) = account {
                account_table.upsert(address, account)?;
            } else if account_table.seek_exact(address)?.is_some() {
                account_table.delete_current()?;
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

            if overlay_storage.erased && storage_table.seek_exact(address)?.is_some() {
                storage_table.delete_current_duplicates()?;
            }

            for (&k, &v) in &overlay_storage.slots {
                upsert_storage_value(&mut storage_table, address, k, v)?;

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
        let mut code_table = self.txn.cursor(tables::Code)?;
        for (code_hash, code) in self.hash_to_code {
            code_table.upsert(code_hash, code)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        h256_to_u256,
        kv::{new_mem_chaindata, tables::BitmapKey},
    };
    use hex_literal::hex;

    #[test]
    fn storage_update() {
        let db = new_mem_chaindata().unwrap();
        let txn = db.begin_mutable().unwrap();

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
            .unwrap();

        txn.set(tables::Storage, address, (location_b, value_b))
            .unwrap();

        let mut buffer = Buffer::new(&txn, None);

        assert_eq!(
            buffer
                .read_storage(address, h256_to_u256(location_a))
                .unwrap(),
            value_a1
        );

        // Update only location A
        buffer
            .update_storage(address, h256_to_u256(location_a), value_a1, value_a2)
            .unwrap();
        buffer.write_to_db().unwrap();

        // Location A should have the new value
        let db_value_a = seek_storage_key(
            &mut txn.cursor(tables::Storage).unwrap(),
            address,
            h256_to_u256(location_a),
        )
        .unwrap()
        .unwrap();
        assert_eq!(db_value_a, value_a2);

        // Location B should not change
        let db_value_b = seek_storage_key(
            &mut txn.cursor(tables::Storage).unwrap(),
            address,
            h256_to_u256(location_b),
        )
        .unwrap()
        .unwrap();
        assert_eq!(db_value_b, value_b);
    }

    #[test]
    fn historical_block() {
        let address = hex!("deadbeef00000000000000000000000000000000").into();
        let location = h256_to_u256(H256(hex!(
            "baadcafe00000000000000000000000000000000000000000000000000000000"
        )));
        let acc_at_10 = Some(Account {
            nonce: 100,
            ..Default::default()
        });
        let acc_at_20 = Some(Account {
            nonce: 200,
            ..Default::default()
        });

        for i in 0..0b1111 {
            let db = new_mem_chaindata().unwrap();
            let txn = db.begin_mutable().unwrap();

            let mut buffer = Buffer::new(&txn, None);
            buffer.begin_block(BlockNumber(10));
            buffer.update_account(address, None, acc_at_10);

            buffer.begin_block(BlockNumber(20));
            buffer.update_account(address, acc_at_10, acc_at_20);
            buffer
                .update_storage(address, location, U256::ZERO, 20.as_u256())
                .unwrap();

            if i & 0b0001 == 0b0001 {
                buffer.write_to_db().unwrap();
                buffer = Buffer::new(&txn, None);
            }

            // 1. Erase and update in the same block
            buffer.begin_block(BlockNumber(30));
            buffer.erase_storage(address).unwrap();
            buffer
                .update_storage(address, location, 20.as_u256(), 30.as_u256())
                .unwrap();

            if i & 0b0010 == 0b0010 {
                buffer.write_to_db().unwrap();
                buffer = Buffer::new(&txn, None);
            }

            // 2. Just erase
            buffer.begin_block(BlockNumber(40));
            buffer.erase_storage(address).unwrap();

            if i & 0b0100 == 0b0100 {
                buffer.write_to_db().unwrap();
                buffer = Buffer::new(&txn, None);
            }

            // 3. First update...
            buffer.begin_block(BlockNumber(50));
            buffer
                .update_storage(address, location, U256::ZERO, 50.as_u256())
                .unwrap();

            if i & 0b1000 == 0b1000 {
                buffer.write_to_db().unwrap();
                buffer = Buffer::new(&txn, None);
            }

            // ...then erase
            buffer.begin_block(BlockNumber(60));
            buffer.erase_storage(address).unwrap();

            buffer.write_to_db().unwrap();

            txn.set(
                tables::AccountHistory,
                BitmapKey {
                    inner: address,
                    block_number: BlockNumber(10),
                },
                [10].into_iter().collect(),
            )
            .unwrap();
            txn.set(
                tables::AccountHistory,
                BitmapKey {
                    inner: address,
                    block_number: BlockNumber(u64::MAX),
                },
                [20].into_iter().collect(),
            )
            .unwrap();
            txn.set(
                tables::StorageHistory,
                BitmapKey {
                    inner: (address, u256_to_h256(location)),
                    block_number: BlockNumber(40),
                },
                [20, 30, 40].into_iter().collect(),
            )
            .unwrap();
            txn.set(
                tables::StorageHistory,
                BitmapKey {
                    inner: (address, u256_to_h256(location)),
                    block_number: BlockNumber(u64::MAX),
                },
                [50, 60].into_iter().collect(),
            )
            .unwrap();

            for (historical_block, expected_account, expected_value) in [
                (0, None, 0),
                (5, None, 0),
                (10, acc_at_10, 0),
                (15, acc_at_10, 0),
                (20, acc_at_20, 20),
                (25, acc_at_20, 20),
                (30, acc_at_20, 30),
                (35, acc_at_20, 30),
                (40, acc_at_20, 0),
                (45, acc_at_20, 0),
                (50, acc_at_20, 50),
                (55, acc_at_20, 50),
                (60, acc_at_20, 0),
                (65, acc_at_20, 0),
            ] {
                // Buffer with the state at the end of `historical_block` block
                let buffer = Buffer::new(&txn, Some(historical_block.into()));

                assert_eq!(buffer.read_account(address).unwrap(), expected_account);
                assert_eq!(
                    buffer.read_storage(address, location).unwrap(),
                    expected_value
                );
            }
        }
    }
}
