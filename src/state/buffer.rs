use crate::{
    accessors,
    kv::{
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
        traits::*,
    },
    models::*,
    state::database::*,
    u256_to_h256, MutableTransaction, State, Transaction,
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
pub type StorageChanges = BTreeMap<Address, BTreeMap<Incarnation, BTreeMap<U256, U256>>>;

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
    storage: HashMap<Address, BTreeMap<Incarnation, HashMap<U256, U256>>>,

    account_changes: BTreeMap<BlockNumber, AccountChanges>, // per block
    storage_changes: BTreeMap<BlockNumber, StorageChanges>, // per block

    incarnations: BTreeMap<Address, Incarnation>,
    hash_to_code: BTreeMap<H256, Bytes>,
    storage_prefix_to_code_hash: BTreeMap<(Address, Incarnation), H256>,

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

        if let Some(enc) = self.txn.get(&tables::Account, address).await? {
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
        location: U256,
    ) -> anyhow::Result<U256> {
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
            return;
        }

        self.accounts.insert(address, current);

        if account_deleted {
            let initial = initial.expect("deleted account must have existed before");
            if initial.incarnation.0 > 0 {
                self.incarnations.insert(address, initial.incarnation);
            }
        }
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
        // Write to state tables
        let mut account_table = self.txn.mutable_cursor(&tables::Account).await?;
        let mut storage_table = self.txn.mutable_cursor_dupsort(&tables::Storage).await?;

        let addresses = self.accounts.keys().chain(self.storage.keys());

        let mut storage_keys = Vec::new();

        for &address in addresses {
            if let Some(account) = self.accounts.get(&address) {
                if account_table.seek_exact(address).await?.is_some() {
                    account_table.delete_current().await?;
                }

                if let Some(account) = account {
                    account_table
                        .upsert(address, account.encode_for_storage(false))
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
                            &mut storage_table,
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
        for (address, incarnation) in self.incarnations {
            incarnation_table.upsert(address, incarnation).await?;
        }

        let mut code_table = self.txn.mutable_cursor(&tables::Code).await?;
        for (code_hash, code) in self.hash_to_code {
            code_table.upsert(code_hash, code).await?;
        }

        let mut code_hash_table = self.txn.mutable_cursor(&tables::PlainCodeHash).await?;
        for ((address, incarnation), code_hash) in self.storage_prefix_to_code_hash {
            code_hash_table
                .upsert((address, incarnation), code_hash)
                .await?;
        }

        let mut account_change_table = self.txn.mutable_cursor(&tables::AccountChangeSet).await?;
        for (block_number, account_entries) in self.account_changes {
            for (address, account) in account_entries {
                account_change_table
                    .upsert(block_number, AccountChange { address, account })
                    .await?;
            }
        }

        let mut storage_change_table = self.txn.mutable_cursor(&tables::StorageChangeSet).await?;
        for (block_number, storage_entries) in self.storage_changes {
            for (address, incarnation_entries) in storage_entries {
                for (incarnation, storage_entries) in incarnation_entries {
                    for (location, value) in storage_entries {
                        let location = u256_to_h256(location);
                        let value = value;
                        storage_change_table
                            .upsert(
                                StorageChangeKey {
                                    block_number,
                                    address,
                                    incarnation,
                                },
                                StorageChange { location, value },
                            )
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{h256_to_u256, kv::traits::*, new_mem_database, DEFAULT_INCARNATION};
    use hex_literal::hex;

    #[tokio::test]
    async fn storage_update() {
        let db = new_mem_database().unwrap();
        let txn = db.begin_mutable().await.unwrap();

        let address: Address = hex!("be00000000000000000000000000000000000000").into();

        let location_a = H256(hex!(
            "0000000000000000000000000000000000000000000000000000000000000013"
        ));
        let value_a1 = 0x6b.into();
        let value_a2 = 0x85.into();

        let location_b = H256(hex!(
            "0000000000000000000000000000000000000000000000000000000000000002"
        ));
        let value_b = 0x132.into();

        txn.set(
            &tables::Storage,
            (address, DEFAULT_INCARNATION),
            (location_a, value_a1),
        )
        .await
        .unwrap();

        txn.set(
            &tables::Storage,
            (address, DEFAULT_INCARNATION),
            (location_b, value_b),
        )
        .await
        .unwrap();

        let mut buffer = Buffer::new(&txn, 0.into(), None);

        assert_eq!(
            buffer
                .read_storage(address, DEFAULT_INCARNATION, h256_to_u256(location_a))
                .await
                .unwrap(),
            value_a1
        );

        // Update only location A
        buffer
            .update_storage(
                address,
                DEFAULT_INCARNATION,
                h256_to_u256(location_a),
                value_a1,
                value_a2,
            )
            .await
            .unwrap();
        buffer.write_to_db().await.unwrap();

        // Location A should have the new value
        let db_value_a = seek_storage_key(
            &mut txn.cursor_dup_sort(&tables::Storage).await.unwrap(),
            address,
            DEFAULT_INCARNATION,
            h256_to_u256(location_a),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_a, value_a2);

        // Location B should not change
        let db_value_b = seek_storage_key(
            &mut txn.cursor_dup_sort(&tables::Storage).await.unwrap(),
            address,
            DEFAULT_INCARNATION,
            h256_to_u256(location_b),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(db_value_b, value_b);
    }
}
