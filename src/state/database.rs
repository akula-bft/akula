use crate::{
    bitmapdb,
    changeset::{AccountHistory, Change, HistoryKind, StorageHistory},
    dbutils,
    kv::tables,
    models::*,
    ChangeSet, MutableCursor, MutableCursorDupSort, MutableTransaction, Transaction,
};
use anyhow::Context;
use arrayref::array_ref;
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
        original: U256,
        value: U256,
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
        _: U256,
        _: U256,
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
    account_changes: HashMap<<AccountHistory as HistoryKind>::Key, Bytes<'static>>,
    storage_changed: HashSet<Address>,
    storage_changes: HashMap<<StorageHistory as HistoryKind>::Key, Bytes<'static>>,
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

    pub fn get_account_changes(&self) -> ChangeSet<'static, AccountHistory> {
        self.account_changes
            .iter()
            .map(|(k, v)| Change::new(*k, v.clone()))
            .collect()
    }

    pub fn get_storage_changes(&self) -> ChangeSet<'static, StorageHistory> {
        self.storage_changes
            .iter()
            .map(|(k, v)| Change::new(*k, v.clone()))
            .collect()
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
                .insert(address, original.encode_for_storage(true).into());
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
            .insert(address, original.encode_for_storage(false).into());

        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
        original: U256,
        value: U256,
    ) -> anyhow::Result<()> {
        if original == value {
            return Ok(());
        }

        let composite_key =
            dbutils::plain_generate_composite_storage_key(address, incarnation, key);

        let mut v = [0; 32];
        original.to_big_endian(&mut v);
        self.storage_changes
            .insert(composite_key, v.to_vec().into());
        self.storage_changed.insert(address);

        Ok(())
    }

    async fn create_contract(&mut self, _: Address) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn write_index<'db: 'tx, 'tx, K: HistoryKind, Tx: MutableTransaction<'db>>(
    tx: &'tx Tx,
    block_number: BlockNumber,
    changes: ChangeSet<'tx, K>,
) -> anyhow::Result<()> {
    let mut buf = vec![];
    for change in changes {
        let k = dbutils::composite_key_without_incarnation::<K>(&change.key);

        let mut index = bitmapdb::get(tx, &K::IndexTable::default(), &k, 0, u64::MAX)
            .await
            .context("failed to find chunk")?;

        index.push(*block_number);

        for (chunk_key, chunk) in bitmapdb::Chunks::new(index, bitmapdb::CHUNK_LIMIT).with_keys(&k)
        {
            buf.clear();
            chunk.serialize_into(&mut buf)?;
            tx.set(&K::IndexTable::default(), &chunk_key, &buf).await?;
        }
    }

    Ok(())
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> WriterWithChangesets
    for ChangeSetWriter<'db, 'tx, Tx>
{
    async fn write_changesets(&mut self) -> anyhow::Result<()> {
        async fn w<
            'cs,
            'tx: 'cs,
            K: HistoryKind,
            C: MutableCursorDupSort<'tx, K::ChangeSetTable>,
        >(
            cursor: &mut C,
            block_number: BlockNumber,
            changes: &'cs ChangeSet<'tx, K>,
        ) -> anyhow::Result<()> {
            let mut prev_k = None;
            // TODO: fix lifetimes to return collect
            let s = K::encode(block_number, changes).collect::<Vec<_>>();
            for (k, v) in s {
                let dup = prev_k.map(|prev_k| k == prev_k).unwrap_or(false);
                if dup {
                    cursor.append_dup(&*k, &*v).await?;
                } else {
                    cursor.append(&*k, &*v).await?;
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

impl Account {
    fn account_data(&self, omit_hashes: bool) -> Bytes<'static> {
        self.encode_for_storage(omit_hashes).into()
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
            .set(&tables::PlainState, address.as_bytes(), &value)
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
            .set(&tables::Code, code_hash.as_bytes(), code)
            .await?;
        self.tx
            .set(
                &tables::PlainCodeHash,
                &dbutils::plain_generate_storage_prefix(address, incarnation),
                code_hash.as_bytes(),
            )
            .await?;

        Ok(())
    }

    async fn delete_account(&mut self, address: Address, original: &Account) -> anyhow::Result<()> {
        self.csw.delete_account(address, original).await?;

        self.tx
            .mutable_cursor(&tables::PlainState)
            .await?
            .delete(address.as_bytes(), &[])
            .await?;
        if original.incarnation.0 > 0 {
            self.tx
                .set(
                    &tables::IncarnationMap,
                    address.as_bytes(),
                    &original.incarnation.to_be_bytes(),
                )
                .await?;
        }

        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: Address,
        incarnation: Incarnation,
        key: H256,
        original: U256,
        value: U256,
    ) -> anyhow::Result<()> {
        self.csw
            .write_account_storage(address, incarnation, key, original, value)
            .await?;

        if original == value {
            return Ok(());
        }

        let composite_key =
            dbutils::plain_generate_composite_storage_key(address, incarnation, key);

        let mut c = self.tx.mutable_cursor(&tables::PlainState).await?;
        if value.is_zero() {
            c.delete(&composite_key, &[]).await?;
        } else {
            c.put(&composite_key, &value_to_bytes(value)).await?;
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

pub async fn read_account_data<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
) -> anyhow::Result<Option<Account>> {
    if let Some(encoded) = tx.get(&tables::PlainState, address.as_bytes()).await? {
        return Account::decode_for_storage(&*encoded);
    }

    Ok(None)
}

pub async fn read_account_storage<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    address: Address,
    incarnation: Incarnation,
    key: H256,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    let composite_key = dbutils::plain_generate_composite_storage_key(address, incarnation, key);
    tx.get(&tables::PlainState, &composite_key).await
}

pub async fn read_account_code<'db: 'tx, 'tx, Tx: Transaction<'db>>(
    tx: &'tx Tx,
    _: Address,
    _: Incarnation,
    code_hash: H256,
) -> anyhow::Result<Option<Bytes<'tx>>> {
    tx.get(&tables::PlainState, code_hash.as_bytes()).await
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
) -> anyhow::Result<Option<u64>> {
    Ok(tx
        .get(&tables::IncarnationMap, address.as_bytes())
        .await?
        .map(|b| u64::from_be_bytes(*array_ref!(&*b, 0, 8))))
}
