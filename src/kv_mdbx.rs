use crate::{traits, Bucket, Cursor, CursorDupSort, DupSort, MutableCursor};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use std::future::Future;
use thiserror::Error;

#[async_trait(?Send)]
impl traits::KV for mdbx::Environment {
    type Tx<'kv> = mdbx::RoTransaction<'kv>;

    async fn begin<'kv>(&'kv self, _flags: u8) -> anyhow::Result<Self::Tx<'kv>> {
        Ok(self.begin_ro_txn()?)
    }
}

#[async_trait(?Send)]
impl traits::MutableKV for mdbx::Environment {
    type MutableTx<'kv> = mdbx::RwTransaction<'kv>;

    async fn begin_mutable<'kv>(&'kv self) -> anyhow::Result<Self::MutableTx<'kv>> {
        Ok(self.begin_rw_transaction()?)
    }
}

#[async_trait(?Send)]
impl<'tx, Txn: 'tx + mdbx::Transaction> traits::Transaction for Txn {
    type Cursor<'tx, B: Bucket> = mdbx::RoCursor<'tx, Txn>;
    type CursorDupSort<'tx, B: Bucket + DupSort> = mdbx::RoCursor<'tx, Txn>;

    async fn cursor<'tx, B: Bucket>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.open_ro_cursor()?)
    }

    async fn cursor_dup_sort<'tx, B: Bucket + DupSort>(
        &'tx self,
    ) -> anyhow::Result<Self::Cursor<'tx, B>> {
        self.cursor::<B>().await
    }
}

#[async_trait(?Send)]
impl<'e> traits::MutableTransaction for mdbx::RwTransaction<'e> {
    type MutableCursor<'tx, B: Bucket> = mdbx::RwCursor<'tx, mdbx::RwTransaction<'e>>;

    async fn mutable_cursor<'tx, B: Bucket>(
        &'tx self,
    ) -> anyhow::Result<Self::MutableCursor<'tx, B>> {
        Ok(self.open_db(Some(B::DB_NAME))?.open_rw_cursor()?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        Ok(self.commit()?)
    }

    async fn bucket_size<B: Bucket>(&self) -> anyhow::Result<u64> {
        let st = self
            .open_db(Some(B::DB_NAME))?
            .ok_or_else(|| anyhow!("no database"))?;
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
    Ok(cursor.get(None, None, mdbx_sys::MDBX_FIRST)?)
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
    if B::AUTO_KEYS_CONVERSION {
        return seek_dupsort::<B>(cursor, key);
    }

    seek_general::<B>(cursor, key)
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
    cursor
        .move_on_next()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn prev<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .move_on_prev()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn last<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .move_on_last()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn current<'tx, Txn: mdbx::Transaction, B: Bucket>(
    cursor: &mut mdbx::RoCursor<'tx, Txn>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .current()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

#[async_trait(?Send)]
impl<'tx, Txn: mdbx::Transaction, B: Bucket> Cursor<'tx, B> for mdbx::RoCursor<'tx, Txn> {
    async fn first(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        first::<B>(self)
    }

    default async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        seek_general::<B>(self, key)
    }

    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        todo!()
    }

    async fn next(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        next::<B>(self)
    }

    async fn prev(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        prev::<B>(self)
    }

    async fn last(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        last::<B>(self)
    }

    async fn current(&mut self) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        current::<B>(self)
    }
}

#[async_trait(?Send)]
impl<'tx, B: Bucket + DupSort> Cursor<'tx, B> for heed::RoCursor<'tx> {
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
        seek_general_dupsort::<B>(self, key)
    }
}

#[async_trait(?Send)]
impl<'tx, B: Bucket + DupSort> CursorDupSort<'tx, B> for heed::RoCursor<'tx> {
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
impl<'tx, B: Bucket> MutableCursor<'tx, B> for heed::RwCursor<'tx> {
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        Ok(self.append(key, value)?)
    }

    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        todo!()
    }

    async fn delete_current(&mut self) -> anyhow::Result<()> {
        self.del_current()?;

        Ok(())
    }

    async fn reserve(&mut self, key: &[u8], n: usize) -> anyhow::Result<Bytes<'tx>> {
        todo!()
    }

    async fn put_current(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.put_current(key, value)?;

        Ok(())
    }

    async fn count(&mut self) -> anyhow::Result<usize> {
        todo!()
    }
}
