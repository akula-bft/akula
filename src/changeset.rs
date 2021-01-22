use crate::{common, dbutils::*, Cursor, CursorDupSort};
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use educe::Educe;
use futures::Stream;
use std::{collections::BTreeSet, fmt::Debug, usize};

mod account;
mod account_utils;
mod storage;
mod storage_utils;
pub use self::{account::*, account_utils::*, storage::*, storage_utils::*};

pub trait WalkStream<Key> = Stream<Item = anyhow::Result<(u64, Key, Bytes)>> + Send;

#[async_trait]
pub trait Walker: Send {
    type Key: Eq + Ord + Send;
    type WalkStream<'w>: WalkStream<Self::Key>;

    fn walk(&mut self, from: u64, to: u64) -> Self::WalkStream<'_>;
    async fn find(&mut self, block_number: u64, k: &Self::Key) -> anyhow::Result<Option<Bytes>>;
}

pub trait ChangeSetBucket: Bucket {
    const TEMPLATE: &'static str;

    type Key: Eq + Ord + AsRef<[u8]> + Send;
    type IndexBucket: Bucket;
    type Walker<'cur, C: 'cur + CursorDupSort>: Walker;
    type EncodedStream<'ch>: EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(cursor: &'cur mut C) -> Self::Walker<'cur, C>;
    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_>;
    fn decode(k: Bytes, v: Bytes) -> (u64, Self::Key, Bytes);
}

impl ChangeSetBucket for buckets::AccountChangeSet {
    const TEMPLATE: &'static str = "acc-ind-";

    type Key = [u8; common::HASH_LENGTH];
    type IndexBucket = buckets::AccountsHistory;
    type Walker<'cur, C: 'cur + CursorDupSort> = impl Walker;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker<'cur, C> {
        AccountChangeSet { c }
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        encode_accounts(block_number, s).map(|(k, v)| (Bytes::copy_from_slice(&k), v))
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Self::Key, Bytes) {
        let (b, k1, v) = from_account_db_format(common::HASH_LENGTH)(k, v);
        let mut k = [0; common::HASH_LENGTH];
        k[..].copy_from_slice(&*k1);
        (b, k, v)
    }
}

impl ChangeSetBucket for buckets::PlainAccountChangeSet {
    const TEMPLATE: &'static str = "acc-ind-";

    type Key = [u8; common::ADDRESS_LENGTH];
    type IndexBucket = buckets::AccountsHistory;
    type Walker<'cur, C: 'cur + CursorDupSort> = AccountChangeSetPlain<'cur, C>;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker<'cur, C> {
        AccountChangeSetPlain { c }
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        encode_accounts(block_number, s).map(|(k, v)| (Bytes::copy_from_slice(&k), v))
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Self::Key, Bytes) {
        let (b, k1, v) = from_account_db_format(common::ADDRESS_LENGTH)(k, v);
        let mut k = [0; common::ADDRESS_LENGTH];
        k[..].copy_from_slice(&*k1);
        (b, k, v)
    }
}

impl ChangeSetBucket for buckets::StorageChangeSet {
    const TEMPLATE: &'static str = "st-ind-";

    type Key = [u8; common::HASH_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
    type IndexBucket = buckets::StorageHistory;
    type Walker<'cur, C: 'cur + CursorDupSort> = impl Walker;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker<'cur, C> {
        StorageChangeSetPlain { c }
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        encode_storage(block_number, s, common::HASH_LENGTH)
            .map(|(k, v)| (Bytes::copy_from_slice(&k), v))
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Self::Key, Bytes) {
        let (b, k1, v) = from_storage_db_format(common::HASH_LENGTH)(k, v);
        let mut k = [0; common::HASH_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
        k[..].copy_from_slice(&k1);
        (b, k, v)
    }
}

impl ChangeSetBucket for buckets::PlainStorageChangeSet {
    const TEMPLATE: &'static str = "st-ind-";

    type Key = [u8; common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
    type IndexBucket = buckets::StorageHistory;
    type Walker<'cur, C: 'cur + CursorDupSort> = StorageChangeSetPlain<'cur, C>;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker<'cur, C> {
        StorageChangeSetPlain { c }
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        encode_storage(block_number, s, common::ADDRESS_LENGTH)
            .map(|(k, v)| (Bytes::copy_from_slice(&k), v))
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Self::Key, Bytes) {
        let (b, k1, v) = from_storage_db_format(common::ADDRESS_LENGTH)(k, v);
        let mut k = [0; common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
        k[..].copy_from_slice(&k1);
        (b, k, v)
    }
}

pub trait ChangeKey = Eq + Ord + AsRef<[u8]>;

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Change<Key: ChangeKey> {
    pub key: Key,
    pub value: Bytes,
}

impl<Key: ChangeKey> Debug for Change<Key> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Change")
            .field("key", &hex::encode(self.key.as_ref()))
            .field("value", &hex::encode(&self.value))
            .finish()
    }
}

impl<Key: ChangeKey> Change<Key> {
    pub fn new(key: Key, value: Bytes) -> Self {
        Self { key, value }
    }
}

pub type ChangeSet<Key> = BTreeSet<Change<Key>>;

pub fn from_account_db_format(addr_size: usize) -> impl Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) {
    move |db_key, db_value| {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

        let mut k = db_value;
        let v = k.split_off(addr_size);

        (block_n, k.into(), v)
    }
}

pub fn from_storage_db_format(addr_size: usize) -> impl Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) {
    let st_sz = addr_size + common::INCARNATION_LENGTH + common::HASH_LENGTH;

    move |db_key, mut db_value| {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

        let mut k = vec![0; st_sz];
        let db_key = &db_key[common::BLOCK_NUMBER_LENGTH..]; // remove block_n bytes

        k[..db_key.len()].copy_from_slice(&db_key[..]);
        k[db_key.len()..].copy_from_slice(&db_value[..common::HASH_LENGTH]);

        let v = db_value.split_off(common::HASH_LENGTH);

        (block_n, k.into(), v)
    }
}
