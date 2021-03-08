//! For info on database buckets see https://github.com/ledgerwatch/turbo-geth/docs/programmers_guide/db_walkthrough.MD

use crate::{Bucket, DupSort};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;

/// Putter wraps the database write operations.
#[async_trait(?Send)]
pub trait Putter {
    /// Put inserts or updates a single entry.
    async fn put<B: Bucket>(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
}

/// Getter wraps the database read operations.
#[async_trait(?Send)]
pub trait Getter {
    type WalkStream<'a>: Stream<Item = (Bytes<'static>, Bytes<'static>)>;
    type MultiWalkStream<'a>: Stream<Item = (usize, Bytes<'static>, Bytes<'static>)>;

    /// Get returns the value for a given key if it's present.
    async fn get<B: Bucket>(&self, key: &[u8]) -> anyhow::Result<Option<Bytes>>;
    /// Get returns prober chunk of index or error if index is not created.
    /// Key must contain 8byte inverted block number in the end.
    async fn get_index_chunk<B: Bucket>(
        &self,
        key: &[u8],
        timestamp: u64,
    ) -> anyhow::Result<Option<Bytes>>;

    /// Has indicates whether a key exists in the database.
    async fn has<B: Bucket>(&self, key: &[u8]) -> anyhow::Result<bool>;

    /// Walk iterates over entries with keys greater or equal to startkey.
    /// Only the keys whose first fixedbits match those of startkey are iterated over.
    /// walker is called for each eligible entry.
    /// If walker returns false or an error, the walk stops.
    async fn walk<B: Bucket>(&self, startkey: &[u8], fixedbits: usize) -> Self::WalkStream<'_>;

    /// MultiWalk is similar to multiple Walk calls folded into one.
    async fn multi_walk<B: Bucket>(
        &self,
        startkey: &[&[u8]],
        fixedbits: &[usize],
    ) -> Self::MultiWalkStream<'_>;
}

/// Deleter wraps the database delete operations.
#[async_trait(?Send)]
pub trait Deleter {
    /// Delete removes a single entry.
    async fn delete<B: Bucket>(&self, k: &[u8], v: &[u8]) -> anyhow::Result<()>;
}

/// Database wraps all database operations. All methods are safe for concurrent use.
#[async_trait(?Send)]
pub trait Database: Getter + Putter + Deleter {
    type DbWithPendingMutations<'db>: DbWithPendingMutations<'db>;

    /// MultiPut inserts or updates multiple entries.
    /// Entries are passed as an array:
    /// bucket0, key0, val0, bucket1, key1, val1, ...
    async fn multi_put(&self, args: &[(&[u8], &[u8])]) -> anyhow::Result<usize>;

    /// Starts in-mem batch
    ///
    /// Common pattern:
    ///
    /// ```norun
    /// let batch = db.new_batch().await;
    /// ... some calculations on `batch`
    /// batch.commit().await;
    /// ```
    async fn new_batch(&self) -> Self::DbWithPendingMutations<'_>;

    async fn begin(&self) -> anyhow::Result<Self::DbWithPendingMutations<'_>>;

    async fn last<B: Bucket>(&self) -> anyhow::Result<(Bytes, Bytes)>;

    /// Defines the size of the data batches should ideally add in one write.
    async fn batch_size_hint(&self) -> usize;

    async fn keys(&self) -> anyhow::Result<Vec<Bytes>>;

    async fn append<B: Bucket>(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;

    async fn append_dup<B: Bucket + DupSort>(&self, key: &[u8], value: &[u8])
        -> anyhow::Result<()>;

    async fn sequence<B: Bucket>(&self, amount: usize) -> anyhow::Result<u64>;
}

#[async_trait(?Send)]
pub trait DbWithPendingMutations<'db>: Database {
    async fn commit(self) -> anyhow::Result<usize>;

    async fn commit_and_begin(&mut self) -> anyhow::Result<()>;

    async fn batch_size(&self) -> usize;

    async fn reserve<B: Bucket>(&self, key: &[u8], i: usize) -> anyhow::Result<Bytes>;
}
