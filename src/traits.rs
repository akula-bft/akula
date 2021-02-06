use crate::dbutils;
use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::Bytes;
use dbutils::{Bucket, DupFixed, DupSort};
use ethereum_types::Address;
use futures::stream::LocalBoxStream;
use std::{cmp::Ordering, pin::Pin};

pub type ComparatorFunc = Pin<Box<dyn Fn(&[u8], &[u8], &[u8], &[u8]) -> Ordering>>;

#[async_trait(?Send)]
pub trait KV {
    type Tx<'kv>: Transaction;
    type MutableTx<'tx>: MutableTransaction;

    async fn view(&self) -> anyhow::Result<Self::Tx<'_>>;
    async fn update(&mut self) -> anyhow::Result<Self::MutableTx<'_>>;

    async fn begin<'parent_tx, 'kv: 'parent_tx>(
        &'kv self,
        parent: Self::Tx<'parent_tx>,
        flags: TxFlags,
    ) -> anyhow::Result<Self::Tx<'parent_tx>>;
}

pub type TxFlags = u8;

#[async_trait(?Send)]
pub trait Transaction {
    type Cursor<'tx>: Cursor<'tx>;
    type CursorDupSort<'tx>: CursorDupSort<'tx>;
    type CursorDupFixed<'tx>: CursorDupFixed<'tx>;

    /// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
    /// If bucket was created with lmdb.DupSort flag, then cursor with interface CursorDupSort created
    /// If bucket was created with lmdb.DupFixed flag, then cursor with interface CursorDupFixed created
    /// Otherwise - object of interface Cursor created
    ///
    /// Cursor, also provides a grain of magic - it can use a declarative configuration - and automatically break
    /// long keys into DupSort key/values. See docs for `bucket.go:BucketConfigItem`
    async fn cursor<'tx, B: Bucket>(&'tx self) -> anyhow::Result<Self::Cursor<'tx>>;
    /// Can be used if bucket has lmdb.DupSort flag.
    async fn cursor_dup_sort<'tx, B: Bucket + DupSort>(
        &'tx self,
    ) -> anyhow::Result<Self::CursorDupSort<'tx>>;
    /// Can be used if bucket has lmdb.DupFixed flag.
    async fn cursor_dup_fixed<'tx, B: Bucket + DupFixed>(
        &'tx self,
    ) -> anyhow::Result<Self::CursorDupFixed<'tx>>;
    async fn get_one<'tx, B: Bucket>(&'tx self, key: &[u8]) -> anyhow::Result<Bytes<'tx>>;
    async fn has_one<B: Bucket>(&self, key: &[u8]) -> anyhow::Result<bool>;
}

#[async_trait(?Send)]
pub trait MutableTransaction: Transaction {}

#[async_trait(?Send)]
pub trait Transaction2: Transaction {
    type CursorDupSort2<'tx>: CursorDupSort2<'tx>;
    type CursorDupFixed2<'tx>: CursorDupFixed2<'tx>;

    async fn cursor_dup_sort2<'tx, B: Bucket + DupSort>(
        &'tx self,
    ) -> anyhow::Result<Self::CursorDupSort2<'tx>>;

    async fn cursor_dup_fixed2<'tx, B: Bucket + DupFixed>(
        &'tx self,
    ) -> anyhow::Result<Self::CursorDupFixed2<'tx>>;

    async fn commit(self) -> anyhow::Result<()>;

    async fn bucket_size<B: Bucket>(&self) -> anyhow::Result<u64>;

    fn comparator<B: Bucket>(&self) -> ComparatorFunc;
    fn cmp<B: Bucket>(a: &[u8], b: &[u8]) -> Ordering;
    fn dcmp<B: Bucket>(a: &[u8], b: &[u8]) -> Ordering;

    /// Allows to create a linear sequence of unique positive integers for each table.
    /// Can be called for a read transaction to retrieve the current sequence value, and the increment must be zero.
    /// Sequence changes become visible outside the current write transaction after it is committed, and discarded on abort.
    /// Starts from 0.
    async fn sequence<B: Bucket>(&self, amount: usize) -> anyhow::Result<usize>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait Cursor<'tx> {
    async fn first(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn seek(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn seek_exact(&mut self, key: &[u8]) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn next(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn prev(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn last(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn current(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait MutableCursor<'tx>: Cursor<'tx> {
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

    async fn reserve(&mut self, key: &[u8], n: usize) -> anyhow::Result<Bytes<'static>>;

    async fn put_current(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;

    /// Fast way to calculate amount of keys in bucket. It counts all keys even if prefix was set.
    async fn count(&mut self) -> anyhow::Result<usize>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait CursorDupSort<'tx>: Cursor<'tx> {
    /// Second parameter can be nil only if searched key has no duplicates, or return error
    async fn seek_both_exact(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    async fn seek_both_range(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    /// Position at first data item of current key
    async fn first_dup(&mut self) -> anyhow::Result<Bytes<'static>>;
    /// Position at next data item of current key
    async fn next_dup(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    /// Position at first data item of next key
    async fn next_no_dup(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
    /// Position at last data item of current key
    async fn last_dup(&mut self, key: &[u8]) -> anyhow::Result<Bytes<'static>>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait CursorDupSort2<'tx>: CursorDupSort<'tx> {
    /// Number of duplicates for the current key
    async fn count_duplicates(&mut self) -> anyhow::Result<usize>;
    /// Deletes all of the data items for the current key
    async fn delete_current_duplicates(&mut self) -> anyhow::Result<()>;
    /// Same as `Cursor::append`, but for sorted dup data
    async fn append_dup(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait CursorDupFixed<'tx>: CursorDupSort<'tx> {
    /// Return up to a page of duplicate data items from current cursor position
    /// After return - move cursor to prepare for `MDB_NEXT_MULTIPLE`
    async fn get_multi(&mut self) -> anyhow::Result<Bytes<'static>>;
    /// Return up to a page of duplicate data items from next cursor position
    /// After return - move cursor to prepare for `MDB_NEXT_MULTIPLE`
    async fn next_multi(&mut self) -> anyhow::Result<(Bytes<'static>, Bytes<'static>)>;
}

#[async_trait(?Send)]
#[auto_impl(&mut, Box)]
pub trait CursorDupFixed2<'tx>: CursorDupFixed<'tx> {
    /// PutMulti store multiple contiguous data elements in a single request.
    /// Panics if `page.len()` is not a multiple of `stride`.
    async fn put_multi(&mut self, key: &[u8], page: &[u8], stride: usize) -> anyhow::Result<()>;
}

#[async_trait(?Send)]
pub trait HasStats: Send {
    /// DB size
    async fn disk_size(&self) -> anyhow::Result<u64>;
}

pub struct SubscribeReply;

#[async_trait(?Send)]
pub trait Backend: Send {
    async fn add_local(&self, v: Bytes) -> anyhow::Result<Bytes<'static>>;
    async fn etherbase(&self) -> anyhow::Result<Address>;
    async fn net_version(&self) -> anyhow::Result<u64>;
    async fn subscribe(&self) -> anyhow::Result<LocalBoxStream<'static, SubscribeReply>>;
}
