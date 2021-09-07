use super::*;
use arrayref::array_ref;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum_types::Address;
use futures_core::stream::{BoxStream, LocalBoxStream};
use std::fmt::Debug;

#[async_trait]
pub trait KV: Send + Sync + 'static {
    type Tx<'db>: Transaction<'db>;

    async fn begin(&self, flags: u8) -> anyhow::Result<Self::Tx<'_>>;
}

#[async_trait]
pub trait MutableKV: KV + 'static {
    type MutableTx<'db>: MutableTransaction<'db>;

    async fn begin_mutable(&self) -> anyhow::Result<Self::MutableTx<'_>>;
}

#[async_trait]
pub trait Transaction<'db>: Send + Sync + Debug + Sized {
    type Cursor<'tx, T: Table>: Cursor<'tx, T>;
    type CursorDupSort<'tx, T: DupSort>: CursorDupSort<'tx, T>;

    async fn cursor<'tx, T>(&'tx self, table: &T) -> anyhow::Result<Self::Cursor<'tx, T>>
    where
        'db: 'tx,
        T: Table;
    async fn cursor_dup_sort<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::CursorDupSort<'tx, T>>
    where
        'db: 'tx,
        T: DupSort;

    async fn get<'tx, T>(&'tx self, table: &T, key: &[u8]) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        'db: 'tx,
        T: Table,
    {
        let mut cursor = self.cursor(table).await?;

        Ok(cursor.seek_exact(key).await?.map(|(_, v)| v))
    }

    async fn read_sequence<'tx, T>(&'tx self, table: &T) -> anyhow::Result<u64>
    where
        T: Table,
    {
        Ok(self
            .cursor(table)
            .await?
            .seek_exact(table.db_name().as_bytes())
            .await?
            .map(|(_, v)| u64::from_be_bytes(*array_ref!(v, 0, 8)))
            .unwrap_or(0))
    }
}

#[async_trait]
pub trait MutableTransaction<'db>: Transaction<'db> {
    type MutableCursor<'tx, T: Table>: MutableCursor<'tx, T>;
    type MutableCursorDupSort<'tx, T: DupSort>: MutableCursorDupSort<'tx, T>;

    async fn mutable_cursor<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursor<'tx, T>>
    where
        'db: 'tx,
        T: Table;
    async fn mutable_cursor_dupsort<'tx, T>(
        &'tx self,
        table: &T,
    ) -> anyhow::Result<Self::MutableCursorDupSort<'tx, T>>
    where
        'db: 'tx,
        T: DupSort;

    async fn set<T: Table>(&self, table: &T, k: &[u8], v: &[u8]) -> anyhow::Result<()>;

    async fn commit(self) -> anyhow::Result<()>;

    /// Allows to create a linear sequence of unique positive integers for each table.
    /// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
    /// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
    /// Starts from 0.
    async fn increment_sequence<T>(&self, table: &T, amount: u64) -> anyhow::Result<u64>
    where
        T: Table,
    {
        let mut c = self.mutable_cursor::<T>(table).await?;

        let current_v = c
            .seek_exact(table.db_name().as_bytes())
            .await?
            .map(|(_, v)| u64::from_be_bytes(*array_ref!(v, 0, 8)))
            .unwrap_or(0);

        c.put(
            table.db_name().as_bytes(),
            &(current_v + amount).to_be_bytes(),
        )
        .await?;

        Ok(current_v)
    }
}

#[async_trait]
pub trait Cursor<'tx, T>: Send + Debug
where
    T: Table,
{
    async fn first(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn next(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn prev(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn last(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    async fn current(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;

    fn walk<'cur, F>(
        &'cur mut self,
        start_key: &[u8],
        take_while: F,
    ) -> BoxStream<'cur, anyhow::Result<(Bytes<'tx>, Bytes<'tx>)>>
    where
        F: Fn(&Bytes<'tx>, &Bytes<'tx>) -> bool + Send + 'cur,
        'tx: 'cur,
    {
        let start_key = start_key.to_vec();
        Box::pin(try_stream! {
            if let Some((mut k, mut v)) = self.seek(&start_key).await? {
                loop {
                    if !(take_while)(&k, &v) {
                        break;
                    }

                    yield (k, v);

                    match self.next().await? {
                        Some((k1, v1)) => {
                            (k, v) = (k1, v1);
                        }
                        None => break,
                    }
                }
            }
        })
    }
}

#[async_trait]
pub trait MutableCursor<'tx, T>: Cursor<'tx, T>
where
    T: Table,
{
    /// Put based on order
    async fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    /// Append the given key/data pair to the end of the database.
    /// This option allows fast bulk loading when keys are already known to be in the correct order.
    async fn append(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    /// Short version of SeekExact+DeleteCurrent or SeekBothExact+DeleteCurrent
    async fn delete(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;

    /// Deletes the key/data pair to which the cursor refers.
    /// This does not invalidate the cursor, so operations such as MDB_NEXT
    /// can still be used on it.
    /// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
    /// this operation.
    async fn delete_current(&mut self) -> anyhow::Result<()>;

    /// Fast way to calculate amount of keys in table. It counts all keys even if prefix was set.
    async fn count(&mut self) -> anyhow::Result<usize>;
}

#[async_trait]
pub trait CursorDupSort<'tx, T>: Cursor<'tx, T>
where
    T: DupSort,
{
    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<Option<Bytes<'tx>>>;
    /// Position at next data item of current key
    async fn next_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
    /// Position at first data item of next key
    async fn next_no_dup(&mut self) -> anyhow::Result<Option<(Bytes<'tx>, Bytes<'tx>)>>;
}

#[async_trait]
pub trait MutableCursorDupSort<'tx, T>: MutableCursor<'tx, T> + CursorDupSort<'tx, T>
where
    T: DupSort,
{
    /// Deletes all of the data items for the current key
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()>;
    /// Same as `Cursor::append`, but for sorted dup data
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
}

#[async_trait]
pub trait HasStats: Send {
    /// DB size
    async fn disk_size(&self) -> anyhow::Result<u64>;
}

#[allow(dead_code)]
pub struct SubscribeReply;

#[async_trait]
pub trait Backend: Send {
    async fn add_local(&self, v: Bytes) -> anyhow::Result<Bytes<'static>>;
    async fn etherbase(&self) -> anyhow::Result<Address>;
    async fn net_version(&self) -> anyhow::Result<u64>;
    async fn subscribe(&self) -> anyhow::Result<LocalBoxStream<'static, SubscribeReply>>;
}
