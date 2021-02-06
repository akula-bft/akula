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

pub trait WalkStream<Key> = Stream<Item = anyhow::Result<(u64, Key, Bytes<'static>)>>;

#[async_trait(?Send)]
pub trait Walker {
    type Key: Eq + Ord;
    type WalkStream<'w>: WalkStream<Self::Key>;

    fn walk(&mut self, from: u64, to: u64) -> Self::WalkStream<'_>;
    async fn find(
        &mut self,
        block_number: u64,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'static>>>;
}

pub trait ChangeSetBucket: Bucket {
    const TEMPLATE: &'static str;

    type Key: Eq + Ord + AsRef<[u8]>;
    type IndexBucket: Bucket;
    type Walker<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>>: Walker;
    type EncodedStream<'ch>: EncodedStream;

    fn walker_adapter<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>>(
        cursor: &'cur mut C,
    ) -> Self::Walker<'cur, 'tx, C>;
    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_>;
    fn decode(k: Bytes<'static>, v: Bytes<'static>) -> (u64, Self::Key, Bytes<'static>);
}

impl ChangeSetBucket for buckets::PlainAccountChangeSet {
    const TEMPLATE: &'static str = "acc-ind-";

    type Key = [u8; common::ADDRESS_LENGTH];
    type IndexBucket = buckets::AccountsHistory;
    type Walker<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>> =
        AccountChangeSetPlain<'cur, 'tx, C>;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>>(
        c: &'cur mut C,
    ) -> Self::Walker<'cur, 'tx, C> {
        AccountChangeSetPlain::new(c)
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        let k = encode_block_number(block_number);

        s.iter().map(move |cs| {
            let mut new_v = vec![0; cs.key.as_ref().len() + cs.value.len()];
            new_v[..cs.key.as_ref().len()].copy_from_slice(cs.key.as_ref());
            new_v[cs.key.as_ref().len()..].copy_from_slice(&*cs.value);

            (Bytes::from(k.to_vec()), new_v.into())
        })
    }

    fn decode(k: Bytes<'static>, v: Bytes<'static>) -> (u64, Self::Key, Bytes<'static>) {
        let (b, k1, v) = from_account_db_format(k, v);
        let mut k = [0; common::ADDRESS_LENGTH];
        k[..].copy_from_slice(&*k1);
        (b, k, v)
    }
}

impl ChangeSetBucket for buckets::PlainStorageChangeSet {
    const TEMPLATE: &'static str = "st-ind-";

    type Key = [u8; common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
    type IndexBucket = buckets::StorageHistory;
    type Walker<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>> =
        StorageChangeSetPlain<'cur, 'tx, C>;
    type EncodedStream<'ch> = impl EncodedStream;

    fn walker_adapter<'cur, 'tx: 'cur, C: 'cur + CursorDupSort<'tx>>(
        c: &'cur mut C,
    ) -> Self::Walker<'cur, 'tx, C> {
        StorageChangeSetPlain::new(c)
    }

    fn encode(block_number: u64, s: &ChangeSet<Self::Key>) -> Self::EncodedStream<'_> {
        s.iter().map(move |cs| {
            let cs_key = cs.key.as_ref();

            let key_part = common::ADDRESS_LENGTH + common::INCARNATION_LENGTH;

            let mut new_k = vec![0; common::BLOCK_NUMBER_LENGTH + key_part];
            new_k[..common::BLOCK_NUMBER_LENGTH]
                .copy_from_slice(&encode_block_number(block_number));
            new_k[common::BLOCK_NUMBER_LENGTH..].copy_from_slice(&cs_key[..key_part]);

            let mut new_v = vec![0; common::HASH_LENGTH + cs.value.len()];
            new_v[..common::HASH_LENGTH].copy_from_slice(&cs_key[key_part..]);
            new_v[common::HASH_LENGTH..].copy_from_slice(&cs.value[..]);

            (new_k.into(), new_v.into())
        })
    }

    fn decode(k: Bytes<'static>, v: Bytes<'static>) -> (u64, Self::Key, Bytes<'static>) {
        let (b, k1, v) = from_storage_db_format(k, v);
        let mut k = [0; common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
        k[..].copy_from_slice(&k1);
        (b, k, v)
    }
}

pub trait ChangeKey = Eq + Ord + AsRef<[u8]>;

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct Change<Key: ChangeKey> {
    pub key: Key,
    pub value: Bytes<'static>,
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
    pub fn new(key: Key, value: Bytes<'static>) -> Self {
        Self { key, value }
    }
}

pub type ChangeSet<Key> = BTreeSet<Change<Key>>;

pub fn from_account_db_format(
    db_key: Bytes<'static>,
    db_value: Bytes<'static>,
) -> (u64, Bytes<'static>, Bytes<'static>) {
    let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

    let mut k = db_value;
    let v = k.split_off(common::ADDRESS_LENGTH);

    (block_n, k.into(), v)
}

pub fn from_storage_db_format(
    db_key: Bytes<'static>,
    mut db_value: Bytes<'static>,
) -> (u64, Bytes<'static>, Bytes<'static>) {
    let st_sz = common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH;

    let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

    let mut k = vec![0; st_sz];
    let db_key = &db_key[common::BLOCK_NUMBER_LENGTH..]; // remove block_n bytes

    k[..db_key.len()].copy_from_slice(&db_key[..]);
    k[db_key.len()..].copy_from_slice(&db_value[..common::HASH_LENGTH]);

    let v = db_value.split_off(common::HASH_LENGTH);

    (block_n, k.into(), v)
}
