use crate::{
    kv::{traits, *},
    Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort, Transaction,
};
use ::mdbx::{DatabaseFlags, EnvironmentKind, TransactionKind, WriteFlags, RO, RW};
use anyhow::Context;
use async_trait::async_trait;
use std::{collections::HashMap, ops::Deref, path::Path, str};
use tables::*;

#[derive(Clone, Debug)]
struct TableObjectWrapper<T>(T);

impl<'tx, T> ::mdbx::TableObject<'tx> for TableObjectWrapper<T>
where
    T: traits::TableDecode,
{
    fn decode(data_val: &[u8]) -> Result<Self, ::mdbx::Error>
    where
        Self: Sized,
    {
        T::decode(data_val)
            .map_err(|e| ::mdbx::Error::DecodeError(e.into()))
            .map(Self)
    }
}

#[derive(Debug)]
pub struct Environment<E: EnvironmentKind> {
    inner: ::mdbx::Environment<E>,
    chart: DatabaseChart,
}

impl<E: EnvironmentKind> Environment<E> {
    fn open(
        mut b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
        ro: bool,
    ) -> anyhow::Result<Self> {
        b.set_max_dbs(std::cmp::max(chart.len(), 1));

        b.set_flags(::mdbx::EnvironmentFlags {
            mode: if ro {
                ::mdbx::Mode::ReadOnly
            } else {
                ::mdbx::Mode::ReadWrite {
                    sync_mode: ::mdbx::SyncMode::Durable,
                }
            },
            no_rdahead: true,
            coalesce: true,
            ..Default::default()
        });

        Ok(Self {
            inner: b
                .open(path)
                .with_context(|| format!("failed to open database at {}", path.display()))?,
            chart,
        })
    }

    pub fn open_ro(
        b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
    ) -> anyhow::Result<Self> {
        Self::open(b, path, chart, true)
    }

    pub fn open_rw(
        b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: DatabaseChart,
    ) -> anyhow::Result<Self> {
        let s = Self::open(b, path, chart.clone(), false)?;

        let tx = s.inner.begin_rw_txn()?;
        for (table, info) in &*chart {
            tx.create_db(
                Some(table),
                if info.dup_sort {
                    DatabaseFlags::DUP_SORT
                } else {
                    DatabaseFlags::default()
                },
            )?;
        }
        tx.commit()?;

        Ok(s)
    }
}

impl<E: EnvironmentKind> Deref for Environment<E> {
    type Target = ::mdbx::Environment<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::KV for Environment<E> {
    type Tx<'tx> = MdbxTransaction<'tx, RO, E>;

    async fn begin(&self) -> anyhow::Result<Self::Tx<'_>> {
        Ok(MdbxTransaction {
            inner: self.inner.begin_ro_txn()?,
            chart: self.chart.clone(),
        })
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::MutableKV for Environment<E> {
    type MutableTx<'tx> = MdbxTransaction<'tx, RW, E>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(MdbxTransaction {
            inner: self.inner.begin_rw_txn()?,
            chart: self.chart.clone(),
        })
    }
}

#[derive(Debug)]
pub struct MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    inner: ::mdbx::Transaction<'env, K, E>,
    chart: DatabaseChart,
}

impl<'env, E> MdbxTransaction<'env, RO, E>
where
    E: EnvironmentKind,
{
    pub fn table_sizes(&self) -> anyhow::Result<HashMap<String, u64>> {
        let mut out = HashMap::new();
        let main_db = self.inner.open_db(None)?;
        let mut cursor = self.inner.cursor(&main_db)?;
        while let Some((table, _)) = cursor.next_nodup::<Vec<u8>, ()>()? {
            let table = String::from_utf8(table)?;
            let db = self
                .inner
                .open_db(Some(&table))
                .with_context(|| format!("failed to open table: {}", table))?;
            let st = self
                .inner
                .db_stat(&db)
                .with_context(|| format!("failed to get stats for table: {}", table))?;

            out.insert(
                table,
                ((st.leaf_pages() + st.branch_pages() + st.overflow_pages())
                    * st.page_size() as usize) as u64,
            );

            unsafe {
                self.inner.close_db(db)?;
            }
        }

        Ok(out)
    }
}

#[async_trait]
impl<'env, K, E> traits::Transaction<'env> for MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    type Cursor<'tx, T: Table> = MdbxCursor<'tx, K>;
    type CursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, K>;

    fn id(&self) -> u64 {
        self.inner.id()
    }

    async fn cursor<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        let table_name = table.db_name();
        Ok(MdbxCursor {
            inner: self
                .inner
                .cursor(&self.inner.open_db(Some(table_name.as_ref()))?)?,
            table_info: self
                .chart
                .get(table_name.as_ref() as &str)
                .cloned()
                .unwrap_or(TableInfo { dup_sort: true }),
            t: table.db_name(),
        })
    }

    async fn cursor_dup_sort<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        self.cursor(table).await
    }

    async fn get<'tx, T: Table>(
        &'tx self,
        table: &T,
        key: T::Key,
    ) -> anyhow::Result<Option<T::Value>> {
        Ok(self
            .inner
            .get::<TableObjectWrapper<_>>(
                &self.inner.open_db(Some(table.db_name().as_ref()))?,
                key.encode().as_ref(),
            )?
            .map(|v| v.0))
    }
}

#[async_trait]
impl<'env, E: EnvironmentKind> traits::MutableTransaction<'env> for MdbxTransaction<'env, RW, E> {
    type MutableCursor<'tx, T: Table> = MdbxCursor<'tx, RW>;
    type MutableCursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, RW>;

    async fn mutable_cursor<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        self.cursor(table).await
    }

    async fn mutable_cursor_dupsort<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursorDupSort<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        self.mutable_cursor(table).await
    }

    async fn set<T>(&self, table: &T, k: T::Key, v: T::Value) -> anyhow::Result<()>
    where
        T: Table,
    {
        if self
            .chart
            .get(&table.db_name().as_ref())
            .map(|info| info.dup_sort)
            .unwrap_or(false)
        {
            return MutableCursor::<T>::put(&mut self.mutable_cursor(table).await?, k, v).await;
        }
        Ok(self.inner.put(
            &self.inner.open_db(Some(table.db_name().as_ref()))?,
            &k.encode(),
            &v.encode(),
            WriteFlags::UPSERT,
        )?)
    }

    async fn del<T>(&self, table: &T, key: T::Key, value: Option<T::Value>) -> anyhow::Result<bool>
    where
        T: Table,
    {
        let mut vref = None;
        let value = value.map(TableEncode::encode);

        if let Some(v) = &value {
            vref = Some(v.as_ref());
        };
        Ok(self.inner.del(
            &self.inner.open_db(Some(table.db_name().as_ref()))?,
            key.encode(),
            vref,
        )?)
    }

    async fn clear_table<T>(&self, table: &T) -> anyhow::Result<()>
    where
        T: Table,
    {
        self.inner
            .clear_db(&self.inner.open_db(Some(table.db_name().as_ref()))?)?;

        Ok(())
    }

    async fn commit(self) -> anyhow::Result<()> {
        self.inner.commit()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct MdbxCursor<'txn, K>
where
    K: TransactionKind,
{
    inner: ::mdbx::Cursor<'txn, K>,
    table_info: TableInfo,
    t: string::String<StaticBytes>,
}

fn map_res_inner<T, E>(
    v: Result<Option<(TableObjectWrapper<T::Key>, TableObjectWrapper<T::Value>)>, E>,
) -> anyhow::Result<Option<(T::Key, T::Value)>>
where
    T: Table,
    <T as Table>::Key: TableDecode,
    E: std::error::Error + Send + Sync + 'static,
{
    if let Some((k, v)) = v? {
        return Ok(Some((k.0, v.0)));
    }

    Ok(None)
}

#[async_trait]
impl<'txn, K, T> Cursor<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.first())?)
    }

    async fn seek(&mut self, key: T::SeekKey) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(
            self.inner.set_range(key.encode().as_ref()),
        )?)
    }

    async fn seek_exact(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(
            self.inner.set_key(key.encode().as_ref()),
        )?)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.next())?)
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.prev())?)
    }

    async fn last(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.last())?)
    }

    async fn current(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.get_current())?)
    }
}

#[async_trait]
impl<'txn, K, T> CursorDupSort<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: T::Key,
        value: T::SeekBothKey,
    ) -> anyhow::Result<Option<T::Value>>
    where
        T::Key: Clone,
    {
        let res = self.inner.get_both_range::<TableObjectWrapper<T::Value>>(
            key.encode().as_ref(),
            value.encode().as_ref(),
        )?;

        if let Some(v) = res {
            return Ok(Some(v.0));
        }

        Ok(None)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.next_dup())?)
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        Ok(map_res_inner::<T, _>(self.inner.next_nodup())?)
    }
}

#[async_trait]
impl<'txn, T> MutableCursor<'txn, T> for MdbxCursor<'txn, RW>
where
    T: Table,
{
    async fn put(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::default(),
        )?)
    }

    async fn upsert(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::UPSERT,
        )?)
    }

    async fn append(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND,
        )?)
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.inner.del(WriteFlags::CURRENT)?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait]
impl<'txn, T> MutableCursorDupSort<'txn, T> for MdbxCursor<'txn, RW>
where
    T: DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.inner.del(WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND_DUP,
        )?)
    }
}
