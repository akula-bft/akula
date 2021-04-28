use std::{collections::HashMap, path::Path};

use crate::{
    kv::traits, Cursor, CursorDupSort, DupSort, MutableCursor, MutableCursorDupSort, Table,
};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use mdbx::{
    Cursor as MdbxCursor, DatabaseFlags, EnvironmentKind, Error as MdbxError, GenericEnvironment,
    Transaction as MdbxTransaction, TransactionKind, WriteFlags, WriteMap, RO, RW,
};

fn set<'txn, K: TransactionKind>(
    c: &mut MdbxCursor<'txn, K>,
    k: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
    Ok(MdbxCursor::set_key(c, k)?)
}

fn get_both_range<'txn, K: TransactionKind>(
    c: &mut MdbxCursor<'txn, K>,
    k: &[u8],
    v: &[u8],
) -> anyhow::Result<Option<Bytes<'txn>>> {
    Ok(MdbxCursor::get_both_range(c, k, v)?)
}

pub struct Environment<E: EnvironmentKind> {
    inner: mdbx::GenericEnvironment<E>,
}

impl<E: EnvironmentKind> Environment<E> {
    pub fn open(
        b: mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
    ) -> anyhow::Result<Self> {
        let env = b.open(path)?;

        let tx = env.begin_rw_txn()?;
        for (&db, &is_dup_sort) in chart {
            tx.create_db(
                Some(db),
                if is_dup_sort {
                    DatabaseFlags::DUP_SORT
                } else {
                    DatabaseFlags::default()
                },
            )?;
        }
        tx.commit()?;

        Ok(Self { inner: env })
    }
}

#[async_trait(?Send)]
impl<E: EnvironmentKind> traits::KV for Environment<E> {
    type Tx<'tx> = MdbxTransaction<'tx, RO, E>;

    async fn begin(&self, _flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        Ok(self.inner.begin_ro_txn()?)
    }
}

#[async_trait(?Send)]
impl<E: EnvironmentKind> traits::MutableKV for Environment<E> {
    type MutableTx<'tx> = MdbxTransaction<'tx, RW, E>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(self.inner.begin_rw_txn()?)
    }
}

#[async_trait(?Send)]
impl<'env, K, E> traits::Transaction<'env> for MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    type Cursor<'tx, B: Table> = MdbxCursor<'tx, K>;
    type CursorDupSort<'tx, B: DupSort> = MdbxCursor<'tx, K>;

    async fn cursor<'tx, B>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, B>>
    where
        'env: 'tx,
        B: Table,
    {
        Ok(self.open_db(Some(B::DB_NAME))?.cursor()?)
    }

    async fn cursor_dup_sort<'tx, B>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, B>>
    where
        'env: 'tx,
        B: DupSort,
    {
        self.cursor::<B>().await
    }

    async fn get_one<'tx, B>(&'tx self, key: &[u8]) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        'env: 'tx,
        B: Table,
    {
        Ok(self.open_db(Some(B::DB_NAME))?.get(key)?)
    }
}

#[async_trait(?Send)]
impl<'env, E: EnvironmentKind> traits::MutableTransaction<'env> for MdbxTransaction<'env, RW, E> {
    type MutableCursor<'tx, B: Table> = MdbxCursor<'tx, RW>;

    async fn mutable_cursor<'tx, B>(&'tx self) -> anyhow::Result<Self::MutableCursor<'tx, B>>
    where
        'env: 'tx,
        B: Table,
    {
        Ok(self
            .create_db(
                Some(B::DB_NAME),
                if impls::impls!(B: DupSort) {
                    DatabaseFlags::DUP_SORT
                } else {
                    DatabaseFlags::default()
                },
            )?
            .cursor()?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        MdbxTransaction::commit(self)?;

        Ok(())
    }

    async fn table_size<B: Table>(&self) -> anyhow::Result<u64> {
        let st = self.open_db(Some(B::DB_NAME))?.stat()?;

        Ok(
            ((st.leaf_pages() + st.branch_pages() + st.overflow_pages()) * st.page_size() as usize)
                as u64,
        )
    }

    async fn sequence<B: Table>(&self, amount: usize) -> anyhow::Result<usize> {
        let mut c = self.mutable_cursor::<B>().await?;

        let current_v = Cursor::<B>::seek_exact(&mut c, B::DB_NAME.as_bytes())
            .await?
            .map(|(k, v)| usize::from_be_bytes(*array_ref!(v, 0, 8)))
            .unwrap_or(0);

        if amount == 0 {
            return Ok(current_v);
        }

        MutableCursor::<B>::put(
            &mut c,
            B::DB_NAME.as_bytes(),
            &(current_v + amount).to_be_bytes(),
        )
        .await?;

        Ok(current_v)
    }
}

#[async_trait(?Send)]
impl<'txn, K, T> Cursor<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::first(self)?)
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn seek_exact(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        set(self, key)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'txn, K, T> CursorDupSort<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<Bytes<'txn>>> {
        get_both_range(self, key, value)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::next_dup(self)?)
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::next_nodup(self)?)
    }
}

#[async_trait(?Send)]
impl<'txn, T> MutableCursor<'txn, T> for MdbxCursor<'txn, RW>
where
    T: Table,
{
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.del(Default::default())?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'txn, B> MutableCursorDupSort<'txn, B> for MdbxCursor<'txn, RW>
where
    B: DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.del(WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND_DUP)?)
    }
}
