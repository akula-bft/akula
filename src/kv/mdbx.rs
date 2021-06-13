use crate::{
    kv::{traits, *},
    Cursor, CursorDupSort, MutableCursor, MutableCursorDupSort,
};
use ::mdbx::{
    DatabaseFlags, EnvironmentKind, Error as MdbxError, Transaction as MdbxTransaction,
    TransactionKind, WriteFlags, RO, RW,
};
use akula_table_defs::AutoDupSortConfig;
use anyhow::{bail, Context};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use std::{collections::HashMap, ops::Deref, path::Path, str};

pub fn table_sizes<K, E>(tx: &MdbxTransaction<K, E>) -> anyhow::Result<HashMap<&'static str, u64>>
where
    K: TransactionKind,
    E: EnvironmentKind,
{
    let mut out = HashMap::new();
    for table in tables::TABLE_MAP.keys() {
        let st = tx
            .db_stat(
                &tx.open_db(Some(table))
                    .with_context(|| format!("failed to open table: {}", table))?,
            )
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
    inner: ::mdbx::Environment<E>,
}

impl<E: EnvironmentKind> Environment<E> {
    fn open(
        mut b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
        ro: bool,
    ) -> anyhow::Result<Self> {
        b.set_max_dbs(chart.len());
        if ro {
            b.set_flags(::mdbx::EnvironmentFlags {
                mode: ::mdbx::Mode::ReadOnly,
                ..Default::default()
            });
        }

        Ok(Self {
            inner: b.open(path).context("failed to open database")?,
        })
    }

    pub fn open_ro(
        b: ::mdbx::EnvironmentBuilder<E>,
        path: &Path,
        chart: &HashMap<&'static str, bool>,
    ) -> anyhow::Result<Self> {
        Self::open(b, path, chart, true)
    }

    pub fn open_rw(
        b: ::mdbx::EnvironmentBuilder<E>,
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
    type Target = ::mdbx::Environment<E>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::KV for Environment<E> {
    type Tx<'tx> = MdbxTransaction<'tx, RO, E>;

    async fn begin(&self, _flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        Ok(self.inner.begin_ro_txn()?)
    }
}

#[async_trait]
impl<E: EnvironmentKind> traits::MutableKV for Environment<E> {
    type MutableTx<'tx> = MdbxTransaction<'tx, RW, E>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(self.inner.begin_rw_txn()?)
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

    async fn cursor<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: Table,
    {
        Ok(MdbxCursor {
            inner: Self::cursor(self, &self.open_db(Some(table.db_name().as_ref()))?)?,
            t: table.db_name(),
        })
    }

    async fn cursor_dup_sort<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'env: 'tx,
        T: DupSort,
    {
        traits::Transaction::cursor(self, table).await
    }

    async fn get<'s, T: Table>(&'s self, table: &T, k: &[u8]) -> anyhow::Result<Option<Bytes<'s>>> {
        Ok(Self::get(
            self,
            &self.open_db(Some(table.db_name().as_ref()))?,
            k,
        )?)
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
        traits::Transaction::cursor(self, table).await
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

    async fn set<T: Table>(&self, table: &T, k: &[u8], v: &[u8]) -> anyhow::Result<()> {
        if tables::DUP_SORT_TABLES
            .get(&table.db_name().as_ref())
            .and_then(|dup| dup.as_ref())
            .is_some()
        {
            return MutableCursor::<T>::put(&mut self.mutable_cursor(table).await?, k, v).await;
        }
        Ok(Self::put(
            self,
            &self.open_db(Some(table.db_name().as_ref()))?,
            k,
            v,
            WriteFlags::UPSERT,
        )?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        MdbxTransaction::commit(self)?;

        Ok(())
    }
}

fn seek_autodupsort<'txn, K: TransactionKind>(
    c: &mut ::mdbx::Cursor<'txn, K>,
    dupsort_data: &AutoDupSortConfig,
    seek: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
    let &AutoDupSortConfig { from, to } = dupsort_data;
    if seek.is_empty() {
        if let Some((mut k, mut v)) = c.first()? {
            if k.len() == to {
                let mut k2 = Vec::with_capacity(k.len() + from - to);
                k2.extend_from_slice(&k[..]);
                k2.extend_from_slice(&v[..from - to]);
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
        seek1 = &seek[..to];
        seek2 = Some(&seek[to..]);
    } else {
        seek1 = seek;
    }

    let (mut k, mut v) = match c.set_range(seek1)? {
        Some(out) => out,
        None => return Ok(None),
    };

    if let Some(seek2) = seek2 {
        if seek1 == k {
            if let Some(out) = c.get_both_range(seek1, seek2)? {
                v = out;
            } else {
                (k, v) = match c.next()? {
                    Some(out) => out,
                    None => return Ok(None),
                };
            }
        }
    }

    if k.len() == to {
        let mut k2 = Vec::with_capacity(k.len() + from - to);
        k2.extend_from_slice(&k);
        k2.extend_from_slice(&v[..from - to]);
        v.advance(from - to);
        k = k2.into();
    }

    Ok(Some((k, v)))
}

fn auto_dup_sort_from_db<'txn>(
    table: &str,
    mut k: Bytes<'txn>,
    mut v: Bytes<'txn>,
) -> (Bytes<'txn>, Bytes<'txn>) {
    if let Some(&AutoDupSortConfig { from, to }) = tables::DUP_SORT_TABLES
        .get(table)
        .and_then(|dup| dup.as_ref())
    {
        if k.len() == to {
            let key_part = from - to;
            k = k[..].iter().chain(&v[..key_part]).copied().collect();
            v.advance(key_part);
        }
    }

    (k, v)
}

#[derive(Debug)]
pub struct MdbxCursor<'txn, K>
where
    K: TransactionKind,
{
    inner: ::mdbx::Cursor<'txn, K>,
    t: string::String<StaticBytes>,
}

#[async_trait]
impl<'txn, K, T> Cursor<'txn, T> for MdbxCursor<'txn, K>
where
    K: TransactionKind,
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Cursor::<T>::seek(self, &[]).await
    }

    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        if let Some(info) = tables::DUP_SORT_TABLES
            .get(&self.t.as_ref())
            .and_then(|dup| dup.as_ref())
        {
            return seek_autodupsort(&mut self.inner, info, key);
        }

        Ok(if key.is_empty() {
            self.inner.first()?
        } else {
            self.inner.set_range(key)?
        })
    }

    async fn seek_exact(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        if let Some(&AutoDupSortConfig { from, to }) = tables::DUP_SORT_TABLES
            .get(&self.t.as_ref())
            .and_then(|dup| dup.as_ref())
        {
            return Ok(self
                .inner
                .get_both_range(&key[..to], &key[to..])?
                .and_then(|v| {
                    (key[to..] == v[..from - to])
                        .then(move || (key[..to].to_vec().into(), v.slice(from - to..)))
                }));
        }

        Ok(self.inner.set_key(key)?)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self
            .inner
            .next()?
            .map(|(k, v)| auto_dup_sort_from_db(self.t.as_ref(), k, v)))
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self
            .inner
            .prev()?
            .map(|(k, v)| auto_dup_sort_from_db(self.t.as_ref(), k, v)))
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self
            .inner
            .last()?
            .map(|(k, v)| auto_dup_sort_from_db(self.t.as_ref(), k, v)))
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self
            .inner
            .get_current()?
            .map(|(k, v)| auto_dup_sort_from_db(self.t.as_ref(), k, v)))
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
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<Bytes<'txn>>> {
        Ok(self.inner.get_both_range(key, value)?)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self.inner.next_dup()?)
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(self.inner.next_nodup()?)
    }
}

fn delete_autodupsort<'txn>(
    c: &mut MdbxCursor<'txn, RW>,
    &AutoDupSortConfig { from, to }: &AutoDupSortConfig,
    key: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "delete from dupsort table {}: can have keys of len=={} and len<{}. key: {},{}",
            c.t,
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() == from {
        if let Some(v) = c.inner.get_both_range(&key[..to], &key[to..])? {
            if v[..from - to] == key[to..] {
                return Ok(c.inner.del(WriteFlags::CURRENT)?);
            }
        }

        return Ok(());
    }

    if c.inner.set(key)?.is_some() {
        c.inner.del(WriteFlags::CURRENT)?;
    }

    Ok(())
}

fn put_autodupsort<'txn>(
    c: &mut MdbxCursor<'txn, RW>,
    &AutoDupSortConfig { from, to }: &AutoDupSortConfig,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<()> {
    if key.len() != from && key.len() >= to {
        bail!(
            "put dupsort table {}: can have keys of len=={} and len<{}. key: {},{}",
            c.t,
            from,
            to,
            hex::encode(key),
            key.len(),
        );
    }

    if key.len() != from {
        match c.inner.put(key, value, WriteFlags::NO_OVERWRITE) {
            Err(MdbxError::KeyExist) => return Ok(c.inner.put(key, value, WriteFlags::CURRENT)?),
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
    let v = match c.inner.get_both_range(key, &value[..from - to])? {
        None => {
            return Ok(c.inner.put(key, value, WriteFlags::default())?);
        }
        Some(v) => v,
    };

    if v[..from - to] == value[..from - to] {
        if v.len() == value.len() {
            // in DupSort case mdbx.Current works only with values of same length
            return Ok(c.inner.put(key, value, WriteFlags::CURRENT)?);
        }
        c.inner.del(WriteFlags::CURRENT)?;
    }

    Ok(c.inner.put(key, value, WriteFlags::default())?)
}

#[async_trait]
impl<'txn, T> MutableCursor<'txn, T> for MdbxCursor<'txn, RW>
where
    T: Table,
{
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        if key.is_empty() {
            bail!("Key must not be empty");
        }

        if let Some(info) = tables::DUP_SORT_TABLES
            .get(&self.t.clone().as_ref())
            .and_then(|dup| dup.as_ref())
        {
            return put_autodupsort(self, info, key, value);
        }

        Ok(self.inner.put(key, value, WriteFlags::default())?)
    }

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.inner.put(&key, &value, WriteFlags::APPEND)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        if let Some(info) = tables::DUP_SORT_TABLES
            .get(&self.t.clone().as_ref())
            .and_then(|dup| dup.as_ref())
        {
            return delete_autodupsort(self, info, key);
        }

        if tables::DUP_SORT_TABLES.contains_key(&self.t.as_ref()) {
            if self.inner.get_both(key, value)?.is_some() {
                self.inner.del(WriteFlags::CURRENT)?;
            }

            return Ok(());
        }

        if self.inner.set(key)?.is_some() {
            self.inner.del(WriteFlags::CURRENT)?;
        }

        return Ok(());
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
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.inner.put(&key, &value, WriteFlags::APPEND_DUP)?)
    }
}
