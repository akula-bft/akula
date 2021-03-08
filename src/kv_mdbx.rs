use crate::{traits, Bucket, Cursor, CursorDupSort, DupSort, MutableCursor};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use mdbx::{Cursor as _, Transaction as _};
use std::future::Future;
use thiserror::Error;

#[async_trait(?Send)]
impl traits::KV for mdbx::Environment {
    type Tx<'kv, 'tx> = mdbx::RoTransaction<'tx>;

    async fn begin<'kv: 'tx, 'tx>(&'kv self, _flags: u8) -> anyhow::Result<Self::Tx<'kv, 'tx>> {
        Ok(self.begin_ro_txn()?)
    }
}

#[async_trait(?Send)]
impl traits::MutableKV for mdbx::Environment {
    type MutableTx<'kv, 'tx> = mdbx::RwTransaction<'tx>;

    async fn begin_mutable<'kv: 'tx, 'tx>(&'kv self) -> anyhow::Result<Self::MutableTx<'kv, 'tx>> {
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
    type MutableCursor<B: Bucket> = mdbx::RwCursor<'tx, mdbx::RwTransaction<'env>>;

    async fn mutable_cursor<B: Bucket>(&'tx self) -> anyhow::Result<Self::MutableCursor<B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.open_rw_cursor()?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        mdbx::Transaction::commit(self)?;

        Ok(())
    }

    async fn bucket_size<B: Bucket>(&self) -> anyhow::Result<u64> {
        // let st = self.open_db(Some(B::DB_NAME))?.stat();
        todo!()
    }

    fn comparator<B: Bucket>(&self) -> crate::ComparatorFunc {
        todo!()
    }

    fn cmp<B: Bucket>(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        todo!()
    }

    fn dcmp<B: Bucket>(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        todo!()
    }

    async fn sequence<B: Bucket>(&self, amount: usize) -> anyhow::Result<usize> {
        todo!()
    }
}

// Cursor

fn first<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // Ok(cursor.get(None, None, mdbx_sys::MDBX_FIRST)?)
    todo!()
}

fn seek_general<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    todo!()
}

fn seek_general_dupsort<'tx, Txn: mdbx::Transaction, B: Bucket + DupSort>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // if B::AUTO_KEYS_CONVERSION {
    //     return seek_dupsort::<'tx, Txn, B>(cursor, key);
    // }

    // seek_general::<'tx, Txn, B>(cursor, key)
    todo!()
}

fn seek_dupsort<'tx, Txn: mdbx::Transaction, B: Bucket + DupSort>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    todo!()
}

fn next<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // cursor
    //     .move_on_next()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn prev<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // cursor
    //     .move_on_prev()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn last<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // cursor
    //     .move_on_last()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

fn current<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    // cursor
    //     .current()?
    //     .map(|(k, v)| (k.into(), v.into()))
    //     .ok_or_else(|| anyhow!("not found"))
    todo!()
}

#[async_trait(?Send)]
impl<'tx, Txn: mdbx::Transaction, B: Bucket> Cursor<'tx, B> for mdbx::RoCursor<'tx, Txn> {
    async fn first(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        first::<Txn, B>(self)
    }

    default async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        seek_general::<Txn, B>(self, key)
    }

    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn next(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        next::<Txn, B>(self)
    }

    async fn prev(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        prev::<Txn, B>(self)
    }

    async fn last(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        last::<Txn, B>(self)
    }

    async fn current(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        current::<Txn, B>(self)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn: mdbx::Transaction, B: Bucket + DupSort> Cursor<'tx, B> for mdbx::RoCursor<'tx, Txn> {
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        seek_general_dupsort::<Txn, B>(self, key)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn: mdbx::Transaction, B: Bucket + DupSort> CursorDupSort<'tx, B>
    for mdbx::RoCursor<'tx, Txn>
{
    async fn seek_both_exact(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn first_dup(&mut self) -> anyhow::Result<Bytes<'tx>> {
        todo!()
    }

    async fn next_dup(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn next_no_dup(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn last_dup(&mut self, key: &[u8]) -> anyhow::Result<Bytes<'tx>> {
        todo!()
    }
}

#[async_trait(?Send)]
impl<'tx, Txn: mdbx::Transaction, B: Bucket> MutableCursor<'tx, B> for mdbx::RwCursor<'tx, Txn> {
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
