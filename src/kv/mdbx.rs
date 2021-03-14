use crate::{
    kv::traits, Bucket, Cursor, CursorDupSort, DupSort, MutableCursor, MutableCursorDupSort,
};
use anyhow::anyhow;
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use mdbx::{Cursor as _, Transaction as _};
use mdbx_sys::{
    MDBX_cursor_op, MDBX_FIRST, MDBX_GET_BOTH_RANGE, MDBX_NEXT_DUP, MDBX_NEXT_NODUP, MDBX_SET,
};
use std::future::Future;
use thiserror::Error;

trait MdbxCursorExt<'txn, Txn>: mdbx::Cursor<'txn, Txn>
where
    Txn: 'txn + mdbx::Transaction,
{
    fn get(
        &mut self,
        k: Option<&[u8]>,
        v: Option<&[u8]>,
        op: MDBX_cursor_op,
    ) -> anyhow::Result<Option<(Option<Bytes<'txn>>, Bytes<'txn>)>> {
        match mdbx::Cursor::get(self, k, v, op) {
            Ok((k, v)) => Ok(Some((k, v))),
            Err(mdbx::Error::NotFound) => Ok(None),
            Err(other) => Err(other.into()),
        }
    }

    fn set(&mut self, k: &[u8]) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(MdbxCursorExt::get(self, Some(k), None, MDBX_SET)?.map(|(k, v)| (k.unwrap(), v)))
    }

    fn get_both_range(
        &mut self,
        k: &[u8],
        v: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>> {
        Ok(
            MdbxCursorExt::get(self, Some(k), Some(v), MDBX_GET_BOTH_RANGE)?
                .map(|(k, v)| (k.unwrap(), v)),
        )
    }
}

impl<'txn, Txn, C> MdbxCursorExt<'txn, Txn> for C
where
    Txn: 'txn + mdbx::Transaction,
    C: mdbx::Cursor<'txn, Txn>,
{
}

#[async_trait(?Send)]
impl traits::KV for mdbx::Environment {
    type Tx<'tx> = mdbx::RoTransaction<'tx>;

    async fn begin(&self, _flags: u8) -> anyhow::Result<Self::Tx<'_>> {
        Ok(self.begin_ro_txn()?)
    }
}

#[async_trait(?Send)]
impl traits::MutableKV for mdbx::Environment {
    type MutableTx<'tx> = mdbx::RwTransaction<'tx>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>> {
        Ok(self.begin_rw_txn()?)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn: 'tx + mdbx::Transaction> traits::Transaction<'tx> for Txn {
    type Cursor<B: Bucket> = mdbx::RoCursor<'tx, Txn>;
    type CursorDupSort<B: Bucket + DupSort> = mdbx::RoCursor<'tx, Txn>;

    async fn cursor<B: Bucket>(&'tx self) -> anyhow::Result<Self::Cursor<B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.open_ro_cursor()?)
    }

    async fn cursor_dup_sort<B: Bucket + DupSort>(&'tx self) -> anyhow::Result<Self::Cursor<B>> {
        self.cursor::<B>().await
    }
}

#[async_trait(?Send)]
impl<'env: 'tx, 'tx> traits::MutableTransaction<'tx> for mdbx::RwTransaction<'env> {
    type MutableCursor<B: Bucket> = mdbx::RwCursor<'tx, Self>;

    async fn mutable_cursor<B: Bucket>(&'tx self) -> anyhow::Result<Self::MutableCursor<B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.open_rw_cursor()?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        mdbx::Transaction::commit(self)?;

        Ok(())
    }

    async fn bucket_size<B: Bucket>(&self) -> anyhow::Result<u64> {
        let st = self.open_db(Some(B::DB_NAME))?.stat()?;

        Ok(
            ((st.leaf_pages() + st.branch_pages() + st.overflow_pages()) * st.page_size() as usize)
                as u64,
        )
    }

    async fn sequence<B: Bucket>(&self, amount: usize) -> anyhow::Result<usize> {
        let mut c = self.mutable_cursor::<B>().await?;

        let current_v = Cursor::<Self, B>::seek_exact(&mut c, B::DB_NAME.as_bytes())
            .await?
            .map(|(k, v)| usize::from_be_bytes(*array_ref!(v, 0, 8)))
            .unwrap_or(0);

        if amount == 0 {
            return Ok(current_v);
        }

        MutableCursor::<Self, B>::put(
            &mut c,
            B::DB_NAME.as_bytes(),
            &(current_v + amount).to_be_bytes(),
        )
        .await?;

        Ok(current_v)
    }
}

// Cursor

fn first<'txn, Txn, B, C>(c: &mut C) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>>
where
    Txn: 'txn + mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'txn, Txn>,
{
    Ok(MdbxCursorExt::get(c, None, None, MDBX_FIRST)?.map(|(k, v)| (k.unwrap(), v)))
}

fn seek_general<'txn, Txn, B, C>(
    c: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'txn, Txn>,
{
    todo!()
}

fn seek_general_dupsort<'tx, Txn, B, C>(
    cursor: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket + DupSort,
    C: mdbx::Cursor<'tx, Txn>,
{
    if B::AUTO_KEYS_CONVERSION.is_some() {
        return seek_dupsort::<Txn, B, C>(cursor, key);
    }

    seek_general::<Txn, B, C>(cursor, key)
}

fn seek_dupsort<'tx, Txn, B, C>(
    cursor: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket + DupSort,
    C: mdbx::Cursor<'tx, Txn>,
{
    todo!()
}

// SeekExact

fn seek_exact<'txn, Txn, B, C>(
    c: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>>
where
    Txn: 'txn + mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'txn, Txn>,
{
    c.set(key)
}

fn seek_exact_dupsort<'txn, Txn, B, C>(
    c: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'txn>, Bytes<'txn>)>>
where
    Txn: 'txn + mdbx::Transaction,
    B: Bucket + DupSort,
    C: mdbx::Cursor<'txn, Txn>,
{
    if let Some((from, to)) = B::AUTO_KEYS_CONVERSION {
        if let Some((k, v)) = c.get_both_range(&key[..to], &key[to..])? {
            if key[to..] == v[..from - to] {
                return Ok(Some((k, v.slice(from - to..))));
            }
        }

        return Ok(None);
    }

    seek_exact::<Txn, B, C>(c, key)
}

fn next<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    todo!()
}

fn prev<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    todo!()
}

fn last<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    todo!()
}

fn current<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    todo!()
}

#[async_trait(?Send)]
impl<'tx, Txn, B, C> Cursor<'tx, Txn, B> for C
where
    Txn: 'tx + mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        first::<Txn, B, Self>(self)
    }

    default async fn seek(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_general::<Txn, B, Self>(self, key)
    }

    default async fn seek_exact(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_exact::<Txn, B, Self>(self, key)
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        next::<Txn, B, Self>(self)
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        prev::<Txn, B, Self>(self)
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        last::<Txn, B, Self>(self)
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        current::<Txn, B, Self>(self)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn, B, C> Cursor<'tx, Txn, B> for C
where
    Txn: 'tx + mdbx::Transaction,
    B: Bucket + DupSort,
    C: mdbx::Cursor<'tx, Txn>,
{
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_general_dupsort::<Txn, B, Self>(self, key)
    }

    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_exact_dupsort::<Txn, B, Self>(self, key)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn, B, C> CursorDupSort<'tx, Txn, B> for C
where
    Txn: 'tx + mdbx::Transaction,
    B: Bucket + DupSort,
    C: mdbx::Cursor<'tx, Txn>,
{
    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        self.get_both_range(key, value)
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        Ok(self
            .get(None, None, MDBX_NEXT_DUP)?
            .map(|(k, v)| (k.unwrap(), v)))
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        Ok(self
            .get(None, None, MDBX_NEXT_NODUP)?
            .map(|(k, v)| (k.unwrap(), v)))
    }
}

#[async_trait(?Send)]
impl<'tx, Txn, B> MutableCursor<'tx, Txn, B> for mdbx::RwCursor<'tx, Txn>
where
    Txn: mdbx::Transaction,
    B: Bucket,
{
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    default async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.put(&key, &value, mdbx::WriteFlags::APPEND)?)
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
impl<'tx, Txn, B> MutableCursorDupSort<'tx, Txn, B> for mdbx::RwCursor<'tx, Txn>
where
    Txn: mdbx::Transaction,
    B: Bucket + DupSort,
{
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()> {
        Ok(self.del(mdbx::WriteFlags::NO_DUP_DATA)?)
    }
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.put(&key, &value, mdbx::WriteFlags::APPEND_DUP)?)
    }
}
