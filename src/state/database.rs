use crate::{
    bitmapdb,
    changeset::{AccountHistory, HistoryKind, StorageHistory},
    kv::{
        tables::{self, BitmapKey},
        Table, TableDecode,
    },
    models::*,
    ChangeSet, CursorDupSort, MutableCursor, MutableCursorDupSort, MutableTransaction, Transaction,
};
use anyhow::Context;
use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::Bytes;
use ethereum_types::*;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

#[async_trait]
#[auto_impl(&)]
pub trait StateReader<'storage> {
    async fn read_account_data(&self, address: Address) -> anyhow::Result<Option<Account>>;
    async fn read_account_storage(
        &self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
    ) -> anyhow::Result<Option<Bytes<'storage>>>;
    async fn read_account_code(
        &self,
        address: Address,
        incarnation: Incarnation,
        code_hash: H256,
    ) -> anyhow::Result<Option<Bytes<'storage>>>;
    async fn read_account_code_size(
        &self,
        address: Address,
        incarnation: Incarnation,
        code_hash: H256,
    ) -> anyhow::Result<usize>;
    async fn read_previous_incarnation(&self, address: Address) -> anyhow::Result<Option<u64>>;
}

#[async_trait]
pub trait StateWriter {
    async fn update_account_data(
        &mut self,
        address: Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()>;
    async fn update_account_code(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        code_hash: H256,
        code: &[u8],
    ) -> anyhow::Result<()>;
    async fn delete_account(&mut self, address: Address, original: &Account) -> anyhow::Result<()>;
    async fn write_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
        original: H256,
        value: H256,
    ) -> anyhow::Result<()>;
    async fn create_contract(&mut self, address: Address) -> anyhow::Result<()>;
}

#[async_trait]
pub trait WriterWithChangesets: StateWriter {
    async fn write_changesets(&mut self) -> anyhow::Result<()>;
    async fn write_history(&mut self) -> anyhow::Result<()>;
}

#[derive(Clone, Default, Debug)]
pub struct Noop;

#[async_trait]
impl StateWriter for Noop {
    async fn update_account_data(
        &mut self,
        _: Address,
        _: &Account,
        _: &Account,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_account_code(
        &mut self,
        _: Address,
        _: Incarnation,
        _: H256,
        _: &[u8],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_account(&mut self, _: Address, _: &Account) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        _: Address,
        _: Incarnation,
        _: H256,
        _: H256,
        _: H256,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_contract(&mut self, _: Address) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
impl WriterWithChangesets for Noop {
    async fn write_changesets(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_history(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct ChangeSetWriter<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> {
    tx: &'tx Tx,
    account_changes:
        HashMap<<AccountHistory as HistoryKind>::Key, <AccountHistory as HistoryKind>::Value>,
    storage_changed: HashSet<Address>,
    storage_changes:
        HashMap<<StorageHistory as HistoryKind>::Key, <StorageHistory as HistoryKind>::Value>,
    block_number: BlockNumber,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> ChangeSetWriter<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_number: impl Into<BlockNumber>) -> Self {
        Self {
            tx,
            account_changes: Default::default(),
            storage_changed: Default::default(),
            storage_changes: Default::default(),
            block_number: block_number.into(),
            _marker: PhantomData,
        }
    }

    pub fn get_account_changes(&self) -> ChangeSet<AccountHistory> {
        self.account_changes
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub fn get_storage_changes(&self) -> ChangeSet<StorageHistory> {
        self.storage_changes.iter().map(|(&k, &v)| (k, v)).collect()
    }
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> StateWriter for ChangeSetWriter<'db, 'tx, Tx> {
    async fn update_account_data(
        &mut self,
        address: Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()> {
        if original != account || self.storage_changed.contains(&address) {
            self.account_changes
                .insert(address, original.encode_for_storage(true));
        }

        Ok(())
    }

    async fn update_account_code(
        &mut self,
        _: Address,
        _: Incarnation,
        _: H256,
        _: &[u8],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_account(&mut self, address: Address, original: &Account) -> anyhow::Result<()> {
        self.account_changes
            .insert(address, original.encode_for_storage(false));

        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
        original: H256,
        value: H256,
    ) -> anyhow::Result<()> {
        if original == value {
            return Ok(());
        }

        self.storage_changes
            .insert((address, incarnation, key), original);
        self.storage_changed.insert(address);

        Ok(())
    }

    async fn create_contract(&mut self, _: Address) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn write_index<'db: 'tx, 'tx, K, Tx>(
    tx: &'tx Tx,
    block_number: BlockNumber,
    changes: ChangeSet<K>,
) -> anyhow::Result<()>
where
    K: HistoryKind,
    BitmapKey<K::IndexChunkKey>: TableDecode,
    Tx: MutableTransaction<'db>,
{
    for (change_key, _) in changes {
        let k = K::index_chunk_key(change_key);
        let mut index = bitmapdb::get(tx, &K::IndexTable::default(), k.clone(), 0, u64::MAX)
            .await
            .context("failed to find chunk")?;

        index.push(*block_number);

        for (chunk_key, chunk) in bitmapdb::Chunks::new(index, bitmapdb::CHUNK_LIMIT).with_keys() {
            tx.set(
                &K::IndexTable::default(),
                (
                    BitmapKey {
                        inner: k.clone(),
                        block_number: chunk_key,
                    },
                    chunk,
                ),
            )
            .await?;
        }
    }

    Ok(())
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> WriterWithChangesets
    for ChangeSetWriter<'db, 'tx, Tx>
{
    async fn write_changesets(&mut self) -> anyhow::Result<()> {
        async fn w<'cs, 'tx: 'cs, K, C>(
            cursor: &mut C,
            block_number: BlockNumber,
            changes: &'cs ChangeSet<K>,
        ) -> anyhow::Result<()>
        where
            K: HistoryKind,
            <K::ChangeSetTable as Table>::Key: Clone + PartialEq,
            C: MutableCursorDupSort<'tx, K::ChangeSetTable>,
        {
            let mut prev_k = None;
            let s = K::encode(block_number, changes).collect::<Vec<_>>();
            for (k, v) in s {
                let dup = prev_k.map(|prev_k| k == prev_k).unwrap_or(false);
                if dup {
                    cursor
                        .append_dup(K::ChangeSetTable::fuse_values(k.clone(), v)?)
                        .await?;
                } else {
                    cursor
                        .append(K::ChangeSetTable::fuse_values(k.clone(), v)?)
                        .await?;
                }

                prev_k = Some(k);
            }

            Ok(())
        }

        w::<AccountHistory, _>(
            &mut self
                .tx
                .mutable_cursor_dupsort(&tables::AccountChangeSet)
                .await?,
            self.block_number,
            &self.get_account_changes(),
        )
        .await?;
        w::<StorageHistory, _>(
            &mut self
                .tx
                .mutable_cursor_dupsort(&tables::StorageChangeSet)
                .await?,
            self.block_number,
            &self.get_storage_changes(),
        )
        .await?;

        Ok(())
    }

    async fn write_history(&mut self) -> anyhow::Result<()> {
        write_index::<AccountHistory, _>(self.tx, self.block_number, self.get_account_changes())
            .await?;
        write_index::<StorageHistory, _>(self.tx, self.block_number, self.get_storage_changes())
            .await?;

        Ok(())
    }
}

pub struct PlainStateWriter<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> {
    tx: &'tx Tx,
    csw: ChangeSetWriter<'db, 'tx, Tx>,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> PlainStateWriter<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_number: impl Into<BlockNumber>) -> Self {
        Self {
            tx,
            csw: ChangeSetWriter::new(tx, block_number),
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> StateWriter for PlainStateWriter<'db, 'tx, Tx> {
    async fn update_account_data(
        &mut self,
        address: Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()> {
        self.csw
            .update_account_data(address, original, account)
            .await?;

        let value = account.encode_for_storage(false);

        self.tx
            .set(
                &tables::PlainState,
                tables::PlainStateFusedValue::Account {
                    address,
                    account: value,
                },
            )
            .await
    }

    async fn update_account_code(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        code_hash: H256,
        code: &[u8],
    ) -> anyhow::Result<()> {
        self.csw
            .update_account_code(address, incarnation, code_hash, code)
            .await?;

        self.tx
            .set(&tables::Code, (code_hash, code.to_vec()))
            .await?;
        self.tx
            .set(&tables::PlainCodeHash, ((address, incarnation), code_hash))
            .await?;

        Ok(())
    }

    async fn delete_account(&mut self, address: Address, original: &Account) -> anyhow::Result<()> {
        self.csw.delete_account(address, original).await?;

        self.tx
            .del(
                &tables::PlainState,
                tables::PlainStateKey::Account(address),
                None,
            )
            .await?;
        if original.incarnation.0 > 0 {
            self.tx
                .set(&tables::IncarnationMap, (address, original.incarnation))
                .await?;
        }

        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        location: H256,
        original: H256,
        value: H256,
    ) -> anyhow::Result<()> {
        self.csw
            .write_account_storage(address, incarnation, location, original, value)
            .await?;

        if original == value {
            return Ok(());
        }

        let mut c = self.tx.mutable_cursor_dupsort(&tables::PlainState).await?;
        if c.seek_storage_key(address, incarnation, location)
            .await?
            .is_some()
        {
            c.delete_current().await?;
        }
        if !value.is_zero() {
            c.put(tables::PlainStateFusedValue::Storage {
                address,
                incarnation,
                location,
                value: value.into(),
            })
            .await?;
        }

        Ok(())
    }

    async fn create_contract(&mut self, address: Address) -> anyhow::Result<()> {
        self.csw.create_contract(address).await
    }
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> WriterWithChangesets
    for PlainStateWriter<'db, 'tx, Tx>
{
    async fn write_changesets(&mut self) -> anyhow::Result<()> {
        self.csw.write_changesets().await
    }

    async fn write_history(&mut self) -> anyhow::Result<()> {
        self.csw.write_history().await
    }
}

#[async_trait]
pub trait PlainStateCursorExt<'tx>: CursorDupSort<'tx, tables::PlainState> {
    async fn seek_storage_key(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        location: H256,
    ) -> anyhow::Result<Option<H256>> {
        if let Some(v) = self
            .seek_both_range(
                tables::PlainStateKey::Storage(address, incarnation),
                location,
            )
            .await?
        {
            if let Some((a, inc, l, v)) = v.as_storage() {
                if a == address && inc == incarnation && l == location {
                    return Ok(Some(v));
                }
            }
        }

        Ok(None)
    }
}

impl<'tx, C: CursorDupSort<'tx, tables::PlainState>> PlainStateCursorExt<'tx> for C {}

pub async fn read_account_data<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    address: Address,
) -> anyhow::Result<Option<Account>> {
    if let Some(encoded) = tx
        .get(&tables::PlainState, tables::PlainStateKey::Account(address))
        .await?
    {
        return Account::decode_for_storage(&*encoded);
    }

    Ok(None)
}

pub async fn read_account_storage<'db, Tx: Transaction<'db>>(
    tx: &Tx,
    address: Address,
    incarnation: Incarnation,
    location: H256,
) -> anyhow::Result<Option<H256>> {
    if let Some(v) = tx
        .cursor_dup_sort(&tables::PlainState)
        .await?
        .seek_both_range(
            tables::PlainStateKey::Storage(address, incarnation),
            location,
        )
        .await?
    {
        if let Some((a, inc, l, v)) = v.as_storage() {
            if a == address && inc == incarnation && l == location {
                return Ok(Some(v));
            }
        }
    }

    Ok(None)
}

pub async fn read_account_code<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    _: Address,
    _: Incarnation,
    code_hash: H256,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    tx.get(&tables::Code, code_hash)
        .await
        .map(|opt| opt.map(|v| v.into()))
}

pub async fn read_account_code_size<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    code_hash: H256,
) -> anyhow::Result<usize> {
    Ok(read_account_code(tx, address, incarnation, code_hash)
        .await?
        .map(|code| code.len())
        .unwrap_or(0))
}

pub async fn read_previous_incarnation<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
) -> anyhow::Result<Option<Incarnation>> {
    Ok(tx.get(&tables::IncarnationMap, address).await?)
}
