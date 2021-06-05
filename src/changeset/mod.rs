use crate::{common, kv::*, CursorDupSort, *};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use std::{collections::BTreeSet, fmt::Debug};

mod account;
mod storage;

pub struct AccountHistory;
pub struct StorageHistory;

pub type AccountChangeSet<'tx> = ChangeSet<'tx, AccountHistory>;
pub type StorageChangeSet<'tx> = ChangeSet<'tx, StorageHistory>;
pub use storage::find_with_incarnation as find_storage_with_incarnation;

pub trait EncodedStream<'tx, 'cs>: Iterator<Item = (Bytes<'tx>, Bytes<'tx>)> + Send + 'cs {}
impl<'tx: 'cs, 'cs, T> EncodedStream<'tx, 'cs> for T where
    T: Iterator<Item = (Bytes<'tx>, Bytes<'tx>)> + Send + 'cs
{
}

pub trait ChangeKey: Eq + Ord + AsRef<[u8]> {}
impl<T> ChangeKey for T where T: Eq + Ord + AsRef<[u8]> {}

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Change<'tx, Key: ChangeKey> {
    pub key: Key,
    pub value: Bytes<'tx>,
}

impl<'tx, Key: ChangeKey> Debug for Change<'tx, Key> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Change")
            .field("key", &hex::encode(self.key.as_ref()))
            .field("value", &hex::encode(&self.value))
            .finish()
    }
}

impl<'tx, Key: ChangeKey> Change<'tx, Key> {
    pub fn new(key: Key, value: Bytes<'tx>) -> Self {
        Self { key, value }
    }
}

pub type ChangeSet<'tx, K> = BTreeSet<Change<'tx, <K as HistoryKind>::Key>>;

#[async_trait]
pub trait HistoryKind: Send {
    type ChangeSetTable: DupSort;
    type Key: Eq + Ord + AsRef<[u8]> + Sync;
    type IndexChunkKey: Eq + Ord + AsRef<[u8]>;
    type IndexTable: Table + Default;
    type EncodedStream<'tx: 'cs, 'cs>: EncodedStream<'tx, 'cs>;

    fn index_chunk_key(key: Self::Key, block_number: u64) -> Self::IndexChunkKey;
    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: u64,
        needle: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, Self::ChangeSetTable>;
    /// Encode changes into DB keys and values
    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        changes: &'cs ChangeSet<'tx, Self>,
    ) -> Self::EncodedStream<'tx, 'cs>;
    /// Decode `Change` from DB keys and values
    fn decode<'tx>(k: Bytes<'tx>, v: Bytes<'tx>) -> (u64, Change<'tx, Self::Key>);
}
