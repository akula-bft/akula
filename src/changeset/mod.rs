use crate::{common, kv::*, CursorDupSort, *};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use std::{collections::BTreeSet, fmt::Debug};

pub mod account;
pub mod storage;

pub trait EncodedStream<'tx, 'cs> = Iterator<Item = (Bytes<'tx>, Bytes<'tx>)> + 'cs;

pub trait ChangeKey = Eq + Ord + AsRef<[u8]>;

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

pub type ChangeSet<'tx, Key> = BTreeSet<Change<'tx, Key>>;

#[async_trait]
pub trait ChangeSetTable: DupSort {
    const TEMPLATE: &'static str;

    type Key: Eq + Ord + AsRef<[u8]>;
    type IndexTable: Table;
    type EncodedStream<'tx: 'cs, 'cs>: EncodedStream<'tx, 'cs>;

    async fn find<'tx, C>(
        cursor: &mut C,
        block_number: u64,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>>
    where
        C: CursorDupSort<'tx, Self>,
        Self: Sized;
    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        s: &'cs ChangeSet<'tx, Self::Key>,
    ) -> Self::EncodedStream<'tx, 'cs>;
    fn decode<'tx>(k: Bytes<'tx>, v: Bytes<'tx>) -> (u64, Self::Key, Bytes<'tx>);
}
