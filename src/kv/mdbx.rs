use crate::{kv::traits, Bucket, Cursor, CursorDupSort, DupSort, MutableCursor};
use anyhow::anyhow;
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use mdbx::{Cursor as _, Transaction as _};
use std::future::Future;
use thiserror::Error;

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

fn first<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    match cursor.get(None, None, mdbx_sys::MDBX_FIRST) {
        Ok((k, v)) => Ok(Some((k.unwrap(), v))),
        Err(mdbx::Error::NotFound) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn seek_general<'tx, Txn, B, C>(
    cursor: &mut C,
    key: &[u8],
) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
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
    if B::AUTO_KEYS_CONVERSION {
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

fn next<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    // cursor
    //     .move_on_next()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn prev<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    // cursor
    //     .move_on_prev()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn last<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    // cursor
    //     .move_on_last()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn current<'tx, Txn, B, C>(cursor: &mut C) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>
where
    Txn: mdbx::Transaction,
    B: Bucket,
    C: mdbx::Cursor<'tx, Txn>,
{
    // cursor
    //     .current()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
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
        first::<Txn, B, C>(self)
    }

    default async fn seek(
        &mut self,
        key: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_general::<Txn, B, C>(self, key)
    }

    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        todo!()
    }

    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        next::<Txn, B, C>(self)
    }

    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        prev::<Txn, B, C>(self)
    }

    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        last::<Txn, B, C>(self)
    }

    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        current::<Txn, B, C>(self)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn, B> Cursor<'tx, Txn, B> for mdbx::RoCursor<'tx, Txn>
where
    Txn: mdbx::Transaction,
    B: Bucket + DupSort,
{
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        seek_general_dupsort::<Txn, B, Self>(self, key)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn, B> CursorDupSort<'tx, Txn, B> for mdbx::RoCursor<'tx, Txn>
where
    Txn: mdbx::Transaction,
    B: Bucket + DupSort,
{
    async fn seek_both_exact(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        todo!()
    }

    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        todo!()
    }

    async fn first_dup(&mut self) -> anyhow::Result<Option<Bytes<'tx>>> {
        todo!()
    }

    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        todo!()
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>> {
        todo!()
    }

    async fn last_dup(&mut self, key: &[u8]) -> anyhow::Result<Option<Bytes<'tx>>> {
        todo!()
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

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.put(&key, &value, mdbx::WriteFlags::APPEND)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.del(Default::default())?;

        Ok(())
    }

    async fn reserve(&mut self, key: &[u8], n: usize) -> anyhow::Result<Bytes<'tx>> {
        todo!()
    }

    async fn put_current(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        mdbx::RwCursor::put(self, &key, &value, mdbx::WriteFlags::CURRENT)?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}
