use crate::{
    bitmapdb,
    changeset::{AccountHistory, Change, HistoryKind, StorageHistory},
    common, dbutils,
    kv::tables,
    models::Account,
    ChangeSet, MutableCursor, MutableCursorDupSort, MutableTransaction,
};
use anyhow::Context;
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
};

#[async_trait]
pub trait StateReader<'storage> {
    async fn read_account_data(&self, address: common::Address) -> anyhow::Result<Option<Account>>;
    async fn read_account_storage(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: common::Hash,
    ) -> anyhow::Result<Option<Bytes<'storage>>>;
    async fn read_account_code(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<Option<Bytes<'storage>>>;
    async fn read_account_code_size(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<usize>;
    async fn read_account_incarnation(
        &self,
        address: common::Address,
    ) -> anyhow::Result<Option<u64>>;
}

#[async_trait]
pub trait StateWriter {
    async fn update_account_data(
        &mut self,
        address: common::Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()>;
    async fn update_account_code(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
        code: &[u8],
    ) -> anyhow::Result<()>;
    async fn delete_account(
        &mut self,
        address: common::Address,
        original: &Account,
    ) -> anyhow::Result<()>;
    async fn write_account_storage(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: common::Hash,
        original: common::Value,
        value: common::Value,
    ) -> anyhow::Result<()>;
    async fn create_contract(&mut self, address: common::Address) -> anyhow::Result<()>;
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
        _: common::Address,
        _: &Account,
        _: &Account,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_account_code(
        &mut self,
        _: common::Address,
        _: common::Incarnation,
        _: common::Hash,
        _: &[u8],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_account(&mut self, _: common::Address, _: &Account) -> anyhow::Result<()> {
        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        _: common::Address,
        _: common::Incarnation,
        _: common::Hash,
        _: common::Value,
        _: common::Value,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn create_contract(&mut self, _: common::Address) -> anyhow::Result<()> {
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
    storage_changed: HashSet<common::Address>,
    storage_changes: HashMap<<StorageHistory as HistoryKind>::Key, Bytes<'static>>,
    block_number: u64,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> ChangeSetWriter<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_number: u64) -> Self {
        Self {
            tx,
            account_changes: Default::default(),
            storage_changed: Default::default(),
            storage_changes: Default::default(),
            block_number,
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
        address: common::Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()> {
        if original != account || self.storage_changed.contains(&address) {
            self.account_changes
                .insert(address, original.account_data(true));
        }

        Ok(())
    }

    async fn update_account_code(
        &mut self,
        _: common::Address,
        _: common::Incarnation,
        _: common::Hash,
        _: &[u8],
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn delete_account(
        &mut self,
        address: common::Address,
        original: &Account,
    ) -> anyhow::Result<()> {
        self.account_changes
            .insert(address, original.account_data(false));

        Ok(())
    }

    async fn write_account_storage(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: common::Hash,
        original: common::Value,
        value: common::Value,
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

    async fn create_contract(&mut self, _: common::Address) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn write_index<'db: 'tx, 'tx, K: HistoryKind, Tx: MutableTransaction<'db>>(
    tx: &'tx Tx,
    block_number: u64,
    changes: ChangeSet<'tx, K>,
) -> anyhow::Result<()> {
    let mut buf = vec![];
    for change in changes {
        let k = dbutils::composite_key_without_incarnation::<K>(&change.key);

        let mut index = bitmapdb::get(tx, &K::IndexTable::default(), &k, 0, u64::MAX)
            .await
            .context("failed to find chunk")?;

        index.push(block_number);

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
            block_number: u64,
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
        if !self.initialised {
            Bytes::new()
        } else {
            let mut acc = self.clone();
            if omit_hashes {
                acc.code_hash = None;
                acc.root = None;
            }

            acc.encode_for_storage().into()
        }
    }
}

pub struct PlainStateWriter<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> {
    tx: &'tx Tx,
    csw: ChangeSetWriter<'db, 'tx, Tx>,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> PlainStateWriter<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx, block_number: u64) -> Self {
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
        address: common::Address,
        original: &Account,
        account: &Account,
    ) -> anyhow::Result<()> {
        self.csw
            .update_account_data(address, original, account)
            .await?;

        let value = account.encode_for_storage();

        self.tx
            .set(&tables::PlainState, address.as_bytes(), &value)
            .await
    }

    async fn update_account_code(
        &mut self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
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

    async fn delete_account(
        &mut self,
        address: common::Address,
        original: &Account,
    ) -> anyhow::Result<()> {
        self.csw.delete_account(address, original).await?;

        self.tx
            .mutable_cursor(&tables::PlainState)
            .await?
            .delete(address.as_bytes(), &[])
            .await?;
        if original.incarnation > 0 {
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
        address: common::Address,
        incarnation: common::Incarnation,
        key: common::Hash,
        original: common::Value,
        value: common::Value,
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
            c.put(&composite_key, &common::value_to_bytes(value))
                .await?;
        }

        Ok(())
    }

    async fn create_contract(&mut self, address: common::Address) -> anyhow::Result<()> {
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

pub struct PlainStateReader<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> {
    tx: &'tx Tx,
    _marker: PhantomData<&'db ()>,
}

impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> PlainStateReader<'db, 'tx, Tx> {
    pub fn new(tx: &'tx Tx) -> Self {
        Self {
            tx,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<'db: 'tx, 'tx, Tx: MutableTransaction<'db>> StateReader<'tx>
    for PlainStateReader<'db, 'tx, Tx>
{
    async fn read_account_data(&self, address: common::Address) -> anyhow::Result<Option<Account>> {
        if let Some(encoded) = self.tx.get(&tables::PlainState, address.as_bytes()).await? {
            return Account::decode_for_storage(&*encoded);
        }

        Ok(None)
    }

    async fn read_account_storage(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        key: common::Hash,
    ) -> anyhow::Result<Option<Bytes<'tx>>> {
        let composite_key =
            dbutils::plain_generate_composite_storage_key(address, incarnation, key);
        self.tx.get(&tables::PlainState, &composite_key).await
    }

    async fn read_account_code(
        &self,
        _: common::Address,
        _: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<Option<Bytes<'tx>>> {
        self.tx.get(&tables::PlainState, code_hash.as_bytes()).await
    }

    async fn read_account_code_size(
        &self,
        address: common::Address,
        incarnation: common::Incarnation,
        code_hash: common::Hash,
    ) -> anyhow::Result<usize> {
        Ok(self
            .read_account_code(address, incarnation, code_hash)
            .await?
            .map(|code| code.len())
            .unwrap_or(0))
    }

    async fn read_account_incarnation(
        &self,
        address: common::Address,
    ) -> anyhow::Result<Option<u64>> {
        Ok(self
            .tx
            .get(&tables::IncarnationMap, address.as_bytes())
            .await?
            .map(|b| u64::from_be_bytes(*array_ref!(&*b, 0, 8))))
    }
}
