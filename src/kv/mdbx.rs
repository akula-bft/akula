use std::{collections::HashMap, ops::Deref, path::Path};

use crate::{
    kv::traits,
    tables::{self, AutoDupSort, AUTO_DUP_SORT, DUPSORT_TABLES},
    Cursor, CursorDupSort, DupSort, MutableCursor, MutableCursorDupSort, Table,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use mdbx::{
    Cursor as MdbxCursor, DatabaseFlags, EnvironmentKind, Error as MdbxError,
    Transaction as MdbxTransaction, TransactionKind, WriteFlags, RO, RW,
};
use tables::TABLE_MAP;

pub fn table_sizes<K, E>(tx: &mdbx::Transaction<K, E>) -> anyhow::Result<HashMap<&'static str, u64>>
where
    K: mdbx::TransactionKind,
    E: mdbx::EnvironmentKind,
{
    let mut out = HashMap::new();
    for (table, _) in TABLE_MAP.iter() {
        let st = tx
            .open_db(Some(table))
            .with_context(|| format!("failed to open table: {}", table))?
            .stat()
            .with_context(|| format!("failed to get stats for table: {}", table))?;

        out.insert(
            *table,
            ((st.leaf_pages() + st.branch_pages() + st.overflow_pages()) * st.page_size() as usize)
                as u64,
        );
    }

    Ok(out)
}

pub struct Environment<E: EnvironmentKind> {
    inner: mdbx::GenericEnvironment<E>,
}

impl<E: EnvironmentKind> Environment<E> {
    fn open(
        mut b: mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
        ro: bool,
    ) -> anyhow::Result<Self> {
        b.set_max_dbs(chart.len());
        if ro {
            b.set_flags(mdbx::EnvironmentFlags {
                mode: mdbx::Mode::ReadOnly,
                ..Default::default()
            });
        }

        Ok(Self {
            inner: b.open(path).context("failed to open database")?,
        })
    }

    pub fn open_ro(
        b: mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
    ) -> anyhow::Result<Self> {
        Self::open(b, path, chart, true)
    }

    pub fn open_rw(
        b: mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
    ) -> anyhow::Result<Self> {
        let s = Self::open(b, path, chart, false)?;

        let tx = s.inner.begin_rw_txn()?;
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

        Ok(s)
    }
}

impl<E: EnvironmentKind> Deref for Environment<E> {
    type Target = mdbx::GenericEnvironment<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
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
    type Cursor<'tx, T: Table> = MdbxCursor<'tx, K>;
    type CursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, K>;

    async fn cursor<'tx, T>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        Ok(self.open_db(Some(T::DB_NAME))?.cursor()?)
    }

    async fn cursor_dup_sort<'tx, T>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        self.cursor::<T>().await
    }

    async fn get<'s, T: Table>(&'s self, k: &[u8]) -> anyhow::Result<Option<Bytes<'s>>> {
        Ok(self.open_db(Some(T::DB_NAME))?.get(k)?)
    }
}

#[async_trait(?Send)]
impl<'env, E: EnvironmentKind> traits::MutableTransaction<'env> for MdbxTransaction<'env, RW, E> {
    type MutableCursor<'tx, T: Table> = MdbxCursor<'tx, RW>;
    type MutableCursorDupSort<'tx, T: DupSort> = MdbxCursor<'tx, RW>;

    async fn mutable_cursor<'tx, T>(&'tx self) -> anyhow::Result<Self::MutableCursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        Ok(self.open_db(Some(T::DB_NAME))?.cursor()?)
    }

    async fn mutable_cursor_dupsort<'tx, T>(
        &'tx self,
    ) -> anyhow::Result<Self::MutableCursorDupSort<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        self.mutable_cursor::<T>().await
    }

    async fn set<T: Table>(&self, k: &[u8], v: &[u8]) -> anyhow::Result<()> {
        if AUTO_DUP_SORT.contains_key(T::DB_NAME) {
            return MutableCursor::<T>::put(&mut self.mutable_cursor::<T>().await?, k, v).await;
        }
        Ok(self
            .open_db(Some(T::DB_NAME))?
            .put(k, v, WriteFlags::UPSERT)?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        MdbxTransaction::commit(self)?;

        Ok(())
    }
}

fn seek_autodupsort<'txn, K: TransactionKind>(
    c: &mut MdbxCursor<'txn, K>,
    dupsort_data: &AutoDupSort,
    seek: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
    let &AutoDupSort { from, to } = dupsort_data;
    if seek.is_empty() {
        if let Some((mut k, mut v)) = c.first()? {
            if k.len() == to {
                let mut k2 = Vec::with_capacity(k.len() + from - to);
                k2[..k.len()].copy_from_slice(&k[..]);
                k2[k.len()..].copy_from_slice(&v[..from - to]);
                v.advance(from - to);
                k = k2.into();
            }
            return Ok(Some((k, v)));
        }

        return Ok(None);
    }

    let seek1;
    let mut seek2 = None;
    if seek.len() > to {
        let s2;
        (seek1, s2) = seek.split_at(to);
        seek2 = Some(s2);
    } else {
        seek1 = seek;
    }
    if let Some((mut k, mut v)) = c.set_range(seek1)? {
        if let Some(seek2) = seek2 {
            if seek1 == k {
                if let Some(mut v2) = c.get_both_range(seek1, seek2)? {
                    if k.len() == to {
                        let mut k2 = Vec::with_capacity(k.len() + from - to);
                        k2[..k.len()].copy_from_slice(&k[..]);
                        k2[k.len()..].copy_from_slice(&v[..from - to]);
                        v2.advance(from - to);
                        v = v2;
                        k = k2.into()
                    }
                } else {
                    return Ok(c.next()?);
                }
            }
        }

        return Ok(Some((k, v)));
    }

    Ok(None)
}

fn auto_dup_sort_from_db<'txn, T: Table>(
    (mut k, mut v): (Bytes<'txn>, Bytes<'txn>),
) -> (Bytes<'txn>, Bytes<'txn>) {
    if let Some(&AutoDupSort { from, to }) = AUTO_DUP_SORT.get(T::DB_NAME) {
        if k.len() == to {
            let key_part = from - to;
            k = k[..].iter().chain(&v[..key_part]).copied().collect();
            v.advance(key_part);
        }
    }

    (k, v)
}

#[async_trait(?Send)]
impl<'txn, K, T> Cursor<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Cursor::<T>::seek(self, &[]).await
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        if let Some(info) = AUTO_DUP_SORT.get(T::DB_NAME) {
            return seek_autodupsort(self, info, key);
        }

        Ok(if key.is_empty() {
            self.first()?
        } else {
            self.set_range(key)?
        })
    }

    async fn seek_exact(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        if let Some(&AutoDupSort { from, to }) = AUTO_DUP_SORT.get(T::DB_NAME) {
            return Ok(self.get_both_range(&key[..to], &key[to..])?.and_then(|v| {
                (key[to..] == v[..from - to])
                    .then(move || (key[..to].to_vec().into(), v.slice(from - to..)))
            }));
        }

        Ok(MdbxCursor::set_key(self, key)?)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::next(self)?.map(auto_dup_sort_from_db::<T>))
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::prev(self)?.map(auto_dup_sort_from_db::<T>))
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::last(self)?.map(auto_dup_sort_from_db::<T>))
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::get_current(self)?.map(auto_dup_sort_from_db::<T>))
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
        Ok(MdbxCursor::get_both_range(self, key, value)?)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::next_dup(self)?)
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursor::next_nodup(self)?)
    }
}

fn delete_autodupsort<'txn>(
    c: &mut MdbxCursor<'txn, RW>,
    &AutoDupSort { from, to }: &AutoDupSort,
    key: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "delete from dupsort bucket: can have keys of len=={} and len<{}. key: {},{}",
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() == from {
        if let Some(v) = MdbxCursor::get_both_range(c, &key[..to], &key[to..])? {
            if v[..from - to] == key[to..] {
                return Ok(MdbxCursor::del(c, WriteFlags::CURRENT)?);
            }
        }

        return Ok(());
    }

    if MdbxCursor::set(c, key)?.is_some() {
        MdbxCursor::del(c, WriteFlags::CURRENT)?;
    }

    Ok(())
}

fn put_autodupsort<'txn>(
    c: &mut MdbxCursor<'txn, RW>,
    &AutoDupSort { from, to }: &AutoDupSort,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "put dupsort: can have keys of len=={} and len<{}. key: {},{}",
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() != from {
        match MdbxCursor::put(c, key, value, WriteFlags::NO_OVERWRITE) {
            Err(MdbxError::KeyExist) => {
                return Ok(MdbxCursor::put(c, key, value, WriteFlags::CURRENT)?)
            }
            Err(e) => {
                return Err(anyhow::Error::from(e).context(format!(
                    "key: {}, val: {}",
                    hex::encode(key),
                    hex::encode(value)
                )))
            }
            Ok(()) => return Ok(()),
        }
    }

    let value = key[to..]
        .iter()
        .chain(value.iter())
        .copied()
        .collect::<Vec<_>>();
    let key = &key[..to];
    let v = match MdbxCursor::get_both_range(c, key, &value[..from - to])? {
        None => {
            return Ok(MdbxCursor::put(c, key, value, WriteFlags::default())?);
        }
        Some(v) => v,
    };

    if v[..from - to] == value[..from - to] {
        if v.len() == value.len() {
            // in DupSort case mdbx.Current works only with values of same length
            return Ok(MdbxCursor::put(c, key, value, WriteFlags::CURRENT)?);
        }
        MdbxCursor::del(c, WriteFlags::CURRENT)?;
    }

    Ok(MdbxCursor::put(c, key, value, WriteFlags::default())?)
}

#[async_trait(?Send)]
impl<'txn, T> MutableCursor<'txn, T> for MdbxCursor<'txn, RW>
where
    T: Table,
{
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        if key.is_empty() {
            bail!("Key must not be empty");
        }

        if let Some(info) = AUTO_DUP_SORT.get(T::DB_NAME) {
            return put_autodupsort(self, info, key, value);
        }

        Ok(MdbxCursor::put(self, key, value, WriteFlags::default())?)
    }

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        if let Some(info) = AUTO_DUP_SORT.get(T::DB_NAME) {
            return delete_autodupsort(self, info, key);
        }

        if DUPSORT_TABLES.contains(&T::DB_NAME) {
            if self.get_both(key, value)?.is_some() {
                MdbxCursor::del(self, WriteFlags::CURRENT)?;
            }

            return Ok(());
        }

        if self.set(key)?.is_some() {
            MdbxCursor::del(self, WriteFlags::CURRENT)?;
        }

        return Ok(());
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.del(WriteFlags::CURRENT)?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'txn, T> MutableCursorDupSort<'txn, T> for MdbxCursor<'txn, RW>
where
    T: DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.del(WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(MdbxCursor::put(self, &key, &value, WriteFlags::APPEND_DUP)?)
    }
}
