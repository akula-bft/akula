use crate::kv::{traits::*, *};
use ::mdbx::{DatabaseFlags, WriteFlags};
pub use ::mdbx::{EnvironmentKind, TransactionKind, RO, RW};
use anyhow::Context;
use std::{collections::HashMap, marker::PhantomData, ops::Deref, path::Path};
use tables::*;

#[derive(Clone, Debug)]
struct TableObjectWrapper<T>(T);

impl<'tx, T> ::mdbx::TableObject<'tx> for TableObjectWrapper<T>
where
    T: TableDecode,
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
pub struct MdbxEnvironment<E: EnvironmentKind> {
    inner: ::mdbx::Environment<E>,
}

impl<E: EnvironmentKind> MdbxEnvironment<E> {
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

impl<E: EnvironmentKind> Deref for MdbxEnvironment<E> {
    type Target = ::mdbx::Environment<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<E: EnvironmentKind> MdbxEnvironment<E> {
    pub fn begin(&self) -> anyhow::Result<MdbxTransaction<'_, RO, E>> {
        Ok(MdbxTransaction {
            inner: self.inner.begin_ro_txn()?,
        })
    }

    pub fn begin_mutable(&self) -> anyhow::Result<MdbxTransaction<'_, RW, E>> {
        Ok(MdbxTransaction {
            inner: self.inner.begin_rw_txn()?,
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

impl<'env, K, E> MdbxTransaction<'env, K, E>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    pub fn cursor<'tx, T>(&'tx self, table: T) -> anyhow::Result<MdbxCursor<'tx, K, T>>
    where
        'env: 'tx,
        T: Table,
    {
        let table_name = table.db_name();
        Ok(MdbxCursor {
            inner: self
                .inner
                .cursor(&self.inner.open_db(Some(table_name.as_ref()))?)?,
            t: table.db_name(),
            _marker: PhantomData,
        })
    }

    pub fn get<T: Table>(&self, table: T, key: T::Key) -> anyhow::Result<Option<T::Value>> {
        Ok(self
            .inner
            .get::<TableObjectWrapper<_>>(
                &self.inner.open_db(Some(table.db_name().as_ref()))?,
                key.encode().as_ref(),
            )?
            .map(|v| v.0))
    }
}

impl<'env, E: EnvironmentKind> MdbxTransaction<'env, RW, E> {
    pub fn set<T>(&self, table: T, k: T::Key, v: T::Value) -> anyhow::Result<()>
    where
        T: Table,
    {
        Ok(self.inner.put(
            &self.inner.open_db(Some(table.db_name().as_ref()))?,
            &k.encode(),
            &v.encode(),
            WriteFlags::UPSERT,
        )?)
    }

    pub fn del<T>(&self, table: T, key: T::Key, value: Option<T::Value>) -> anyhow::Result<bool>
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

    pub fn clear_table<T>(&self, table: T) -> anyhow::Result<()>
    where
        T: Table,
    {
        self.inner
            .clear_db(&self.inner.open_db(Some(table.db_name().as_ref()))?)?;

        Ok(())
    }

    pub fn commit(self) -> anyhow::Result<()> {
        self.inner.commit()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct MdbxCursor<'txn, K, T>
where
    K: TransactionKind,
    T: Table,
{
    inner: ::mdbx::Cursor<'txn, K>,
    t: string::String<Bytes>,
    _marker: PhantomData<T>,
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

impl<'txn, K, T> MdbxCursor<'txn, K, T>
where
    K: TransactionKind,
    T: Table,
{
    pub fn first(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.first())
    }

    pub fn seek(&mut self, key: T::SeekKey) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.set_range(key.encode().as_ref()))
    }

    pub fn seek_exact(&mut self, key: T::Key) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.set_key(key.encode().as_ref()))
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.next())
    }

    pub fn prev(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.prev())
    }

    pub fn last(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.last())
    }

    pub fn current(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.get_current())
    }

    pub fn walk(
        mut self,
        start_key: Option<T::SeekKey>,
    ) -> impl Iterator<Item = anyhow::Result<(T::Key, T::Value)>>
    where
        T: Table,
        T::Key: TableDecode,
    {
        TryGenIter::from(move |_| {
            let start = if let Some(start_key) = start_key {
                self.seek(start_key)?
            } else {
                self.first()?
            };
            if let Some(mut fv) = start {
                loop {
                    yield fv;

                    match self.next()? {
                        Some(fv1) => {
                            fv = fv1;
                        }
                        None => break,
                    }
                }
            }

            Ok(())
        })
    }

    pub fn walk_back(
        mut self,
        start_key: Option<T::SeekKey>,
    ) -> impl Iterator<Item = anyhow::Result<(T::Key, T::Value)>>
    where
        T: Table,
        T::Key: TableDecode,
    {
        TryGenIter::from(move |_| {
            let start = if let Some(start_key) = start_key {
                self.seek(start_key)?
            } else {
                self.last()?
            };
            if let Some(mut fv) = start {
                loop {
                    yield fv;

                    match self.prev()? {
                        Some(fv1) => {
                            fv = fv1;
                        }
                        None => break,
                    }
                }
            }

            Ok(())
        })
    }
}

impl<'txn, K, T> MdbxCursor<'txn, K, T>
where
    K: TransactionKind,
    T: DupSort,
{
    pub fn seek_both_range(
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

    pub fn last_dup(&mut self) -> anyhow::Result<Option<T::Value>>
    where
        T::Key: TableDecode,
    {
        Ok(self
            .inner
            .last_dup::<TableObjectWrapper<T::Value>>()?
            .map(|v| v.0))
    }

    pub fn next_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.next_dup())
    }

    pub fn next_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.next_nodup())
    }

    pub fn prev_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.prev_dup())
    }

    pub fn prev_no_dup(&mut self) -> anyhow::Result<Option<(T::Key, T::Value)>>
    where
        T::Key: TableDecode,
    {
        map_res_inner::<T, _>(self.inner.prev_nodup())
    }

    /// Walk over duplicates for some specific key.
    pub fn walk_dup(mut self, start_key: T::Key) -> impl Iterator<Item = anyhow::Result<T::Value>>
    where
        T::Key: TableDecode,
    {
        TryGenIter::from(move |_| {
            let start = self.seek_exact(start_key)?.map(|(_, v)| v);
            if let Some(mut value) = start {
                loop {
                    yield value;

                    match self.next_dup()? {
                        Some((_, v)) => {
                            value = v;
                        }
                        None => break,
                    }
                }
            }

            Ok(())
        })
    }

    /// Walk over duplicates for some specific key.
    pub fn walk_back_dup(
        mut self,
        start_key: T::Key,
    ) -> impl Iterator<Item = anyhow::Result<T::Value>>
    where
        T::Key: TableDecode,
    {
        TryGenIter::from(move |_| {
            if self.seek_exact(start_key)?.is_some() {
                if let Some(mut value) = self.last_dup()? {
                    loop {
                        yield value;

                        match self.prev_dup()? {
                            Some((_, v)) => {
                                value = v;
                            }
                            None => break,
                        }
                    }
                }
            }

            Ok(())
        })
    }
}

impl<'txn, T> MdbxCursor<'txn, RW, T>
where
    T: Table,
{
    pub fn put(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::default(),
        )?)
    }

    pub fn upsert(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::UPSERT,
        )?)
    }

    pub fn append(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND,
        )?)
    }

    pub fn delete_current(&mut self) -> anyhow::Result<()> {
        self.inner.del(WriteFlags::CURRENT)?;

        Ok(())
    }
}

impl<'txn, T> MdbxCursor<'txn, RW, T>
where
    T: DupSort,
{
    pub fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.inner.del(WriteFlags::NO_DUP_DATA)?)
    }
    pub fn append_dup(&mut self, key: T::Key, value: T::Value) -> anyhow::Result<()> {
        Ok(self.inner.put(
            key.encode().as_ref(),
            value.encode().as_ref(),
            WriteFlags::APPEND_DUP,
        )?)
    }
}
