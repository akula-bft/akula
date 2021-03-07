use crate::{traits, Bucket, Cursor, CursorDupSort, DupSort, MutableCursor};
use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::LocalBoxFuture;
use std::future::Future;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error)]
pub struct NoDatabaseError;

#[async_trait(?Send)]
impl traits::KV for heed::Env {
    type Tx<'kv> = EnvWrapper<'kv, heed::RoTxn<'kv>>;

    async fn begin<'kv>(&'kv self, _flags: u8) -> anyhow::Result<Self::Tx<'kv>> {
        Ok(EnvWrapper {
            env: self,
            v: self.read_txn()?,
        })
    }
}

#[async_trait(?Send)]
impl traits::MutableKV for heed::Env {
    type MutableTx<'kv> = EnvWrapper<'kv, heed::RwTxn<'kv, 'kv>>;

    async fn begin_mutable<'kv>(&'kv self) -> anyhow::Result<Self::MutableTx<'kv>> {
        Ok(EnvWrapper {
            env: self,
            v: self.write_txn()?,
        })
    }
}

pub struct EnvWrapper<'e, T> {
    env: &'e heed::Env,
    v: T,
}

#[async_trait(?Send)]
impl<'e> traits::Transaction for EnvWrapper<'e, heed::RoTxn<'e>> {
    type Cursor<'tx, B: Bucket> = heed::RoCursor<'tx>;
    type CursorDupSort<'tx, B: Bucket + DupSort> = heed::RoCursor<'tx>;

    async fn cursor<'tx, B: Bucket>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, B>> {
        Ok(self
            .env
            .open_database::<(), ()>(Some(B::DB_NAME))?
            .ok_or_else(|| anyhow!("no database"))?
            .cursor(&self.v)?)
    }

    async fn cursor_dup_sort<'tx, B: Bucket + DupSort>(
        &'tx self,
    ) -> anyhow::Result<Self::Cursor<'tx, B>> {
        self.cursor::<B>().await
    }
}

#[async_trait(?Send)]
impl<'e> traits::Transaction for EnvWrapper<'e, heed::RwTxn<'e, 'e>> {
    type Cursor<'tx, B: Bucket> = heed::RoCursor<'tx>;
    type CursorDupSort<'tx, B: Bucket + DupSort> = heed::RoCursor<'tx>;

    async fn cursor<'tx, B: Bucket>(&'tx self) -> anyhow::Result<Self::Cursor<'tx, B>> {
        Ok(self
            .env
            .open_database::<(), ()>(Some(B::DB_NAME))?
            .ok_or_else(|| anyhow!("no database"))?
            .cursor(&self.v)?)
    }

    async fn cursor_dup_sort<'tx, B: Bucket + DupSort>(
        &'tx self,
    ) -> anyhow::Result<Self::Cursor<'tx, B>> {
        self.cursor::<B>().await
    }
}

#[async_trait(?Send)]
impl<'e> traits::MutableTransaction for EnvWrapper<'e, heed::RwTxn<'e, 'e>> {
    type MutableCursor<'tx, B: Bucket> = heed::RwCursor<'tx>;

    async fn mutable_cursor<'tx, B: Bucket>(
        &'tx self,
    ) -> anyhow::Result<Self::MutableCursor<'tx, B>> {
        Ok(self
            .env
            .open_database::<(), ()>(Some(B::DB_NAME))?
            .ok_or_else(|| anyhow!("no database"))?
            .cursor_mut(&self.v)?)
    }

    async fn commit(self) -> anyhow::Result<()> {
        Ok(self.v.commit()?)
    }

    async fn bucket_size<B: Bucket>(&self) -> anyhow::Result<u64> {
        let st = self
            .env
            .open_database(Some(B::DB_NAME))?
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

fn first<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    Ok(cursor
        .move_on_first()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))?)
}

fn seek_general<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    todo!()
}

fn seek_general_dupsort<'tx, B: Bucket + DupSort>(
    cursor: &mut heed::RoCursor<'tx>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    if B::AUTO_KEYS_CONVERSION {
        return seek_dupsort::<B>(cursor, key);
    }

    seek_general::<B>(cursor, key)
}

fn seek_dupsort<'tx, B: Bucket + DupSort>(
    cursor: &mut heed::RoCursor<'tx>,
    key: &[u8],
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    todo!()
}

fn next<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .move_on_next()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn prev<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .move_on_prev()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn last<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .move_on_last()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

fn current<'tx, B: Bucket>(
    cursor: &mut heed::RoCursor<'tx>,
) -> anyhow::Result<(Bytes<'tx>, Bytes<'tx>)> {
    cursor
        .current()?
        .map(|(k, v)| (k.into(), v.into()))
        .ok_or_else(|| anyhow!("not found"))
}

#[async_trait(?Send)]
impl<'tx, B: Bucket> Cursor<'tx, B> for heed::RoCursor<'tx> {
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
