use crate::{
    kv::{tables::BitmapKey, *},
    models::*,
    CursorDupSort,
};
use async_trait::async_trait;
use roaring::RoaringTreemap;
use std::{collections::BTreeSet, fmt::Debug};

mod account;
mod storage;

pub const DEFAULT_INCARNATION: Incarnation = Incarnation(1);

pub struct AccountHistory;
pub struct StorageHistory;

pub type AccountChangeSet = ChangeSet<AccountHistory>;
pub type StorageChangeSet = ChangeSet<StorageHistory>;

pub trait EncodedStream<'cs, T: Table>: Iterator<Item = (T::Key, T::Value)> + Send + 'cs {}
impl<'cs, S, T> EncodedStream<'cs, T> for S
where
    S: Iterator<Item = (T::Key, T::Value)> + Send + 'cs,
    T: Table,
{
}

pub trait ChangeKey: Eq + Ord + Debug {}
impl<T> ChangeKey for T where T: Eq + Ord + Debug {}

pub type Change<K, V> = (K, V);

pub type ChangeSet<K> = BTreeSet<Change<<K as HistoryKind>::Key, <K as HistoryKind>::Value>>;

#[async_trait]
pub trait HistoryKind: Send {
    type Key: Eq + Ord + Sync;
    type Value: Debug + Sync;
    type ChangeSetTable: DupSort;
    type IndexChunkKey: Clone + PartialEq + Send + Sync;
    type IndexTable: Table<
            Key = BitmapKey<Self::IndexChunkKey>,
            Value = RoaringTreemap,
            SeekKey = BitmapKey<Self::IndexChunkKey>,
        > + Default;
    type EncodedStream<'cs>: EncodedStream<'cs, Self::ChangeSetTable>;

    fn index_chunk_key(key: Self::Key) -> Self::IndexChunkKey;
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: BlockNumber,
        needle: Self::Key,
    ) -> anyhow::Result<Option<Self::Value>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>;
    /// Encode changes into DB keys and values
    fn encode(block_number: BlockNumber, changes: &ChangeSet<Self>) -> Self::EncodedStream<'_>;
    /// Decode `Change` from DB keys and values
    fn decode(
        k: <Self::ChangeSetTable as Table>::Key,
        v: <Self::ChangeSetTable as Table>::Value,
    ) -> (BlockNumber, Change<Self::Key, Self::Value>);
}
