use crate::{
    kv::{tables::BitmapKey, *},
    models::*,
    CursorDupSort,
};
use async_trait::async_trait;
use roaring::RoaringTreemap;
use std::{collections::BTreeMap, fmt::Debug};

mod account;
mod storage;

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

pub type ChangeSet<K> = BTreeMap<<K as HistoryKind>::Key, <K as HistoryKind>::Value>;

#[async_trait]
pub trait HistoryKind: Send {
    type Key: Eq + Ord + Sync;
    type Value: Debug + Sync;
    type ChangeSetTable: DupSort;
    type IndexTable: Table<Key = BitmapKey<Self::Key>, Value = RoaringTreemap, SeekKey = BitmapKey<Self::Key>>
        + Default;
    type EncodedStream<'cs>: EncodedStream<'cs, Self::ChangeSetTable>;

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
    ) -> (BlockNumber, (Self::Key, Self::Value));
}
