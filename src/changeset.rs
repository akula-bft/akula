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

pub trait WalkStream<'tx, Key> = Stream<Item = anyhow::Result<(u64, Key, Bytes<'tx>)>>;

#[async_trait(?Send)]
pub trait Walker<'tx> {
    type Key: Eq + Ord;

    async fn find(
        &mut self,
        block_number: u64,
        k: &Self::Key,
    ) -> anyhow::Result<Option<Bytes<'tx>>>;
}

pub trait ChangeSetBucket: Bucket + DupSort {
    const TEMPLATE: &'static str;

    type Key: Eq + Ord + AsRef<[u8]>;
    type IndexBucket: Bucket;
    type Walker<'tx, C: CursorDupSort<'tx, Self>>: Walker<'tx>;
    type EncodedStream<'tx: 'cs, 'cs>: EncodedStream<'tx, 'cs>;

    fn walker_adapter<'tx, C: CursorDupSort<'tx, Self>>(cursor: C) -> Self::Walker<'tx, C>
    where
        Self: Sized;
    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        s: &'cs ChangeSet<'tx, Self::Key>,
    ) -> Self::EncodedStream<'tx, 'cs>;
    fn decode<'tx>(k: Bytes<'tx>, v: Bytes<'tx>) -> (u64, Self::Key, Bytes<'tx>);
}

impl ChangeSetBucket for buckets::PlainAccountChangeSet {
    const TEMPLATE: &'static str = "acc-ind-";

    type Key = [u8; common::ADDRESS_LENGTH];
    type IndexBucket = buckets::AccountsHistory;
    type Walker<'tx, C: CursorDupSort<'tx, Self>> = AccountChangeSetPlain<'tx, C>;
    type EncodedStream<'tx: 'cs, 'cs> = impl EncodedStream<'tx, 'cs>;

    fn walker_adapter<'tx, C: CursorDupSort<'tx, Self>>(c: C) -> Self::Walker<'tx, C> {
        AccountChangeSetPlain::new(c)
    }

    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        s: &'cs ChangeSet<'tx, Self::Key>,
    ) -> Self::EncodedStream<'tx, 'cs> {
        let k = encode_block_number(block_number);

        s.iter().map(move |cs| {
            let mut new_v = vec![0; cs.key.as_ref().len() + cs.value.len()];
            new_v[..cs.key.as_ref().len()].copy_from_slice(cs.key.as_ref());
            new_v[cs.key.as_ref().len()..].copy_from_slice(&*cs.value);

            (Bytes::from(k.to_vec()), new_v.into())
        })
    }

    fn decode<'tx>(k: Bytes<'tx>, v: Bytes<'tx>) -> (u64, Self::Key, Bytes<'tx>) {
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
    type Walker<'tx, C: CursorDupSort<'tx, Self>> = StorageChangeSetPlain<'tx, C>;
    type EncodedStream<'tx: 'cs, 'cs> = impl EncodedStream<'tx, 'cs>;

    fn walker_adapter<'tx, C: CursorDupSort<'tx, Self>>(c: C) -> Self::Walker<'tx, C> {
        StorageChangeSetPlain::new(c)
    }

    fn encode<'cs, 'tx: 'cs>(
        block_number: u64,
        s: &'cs ChangeSet<'tx, Self::Key>,
    ) -> Self::EncodedStream<'tx, 'cs> {
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

    fn decode<'tx>(k: Bytes<'tx>, v: Bytes<'tx>) -> (u64, Self::Key, Bytes<'tx>) {
        let (b, k1, v) = from_storage_db_format(k, v);
        let mut k = [0; common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH];
        k[..].copy_from_slice(&k1);
        (b, k, v)
    }
}

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

pub fn from_account_db_format<'tx>(
    db_key: Bytes<'tx>,
    db_value: Bytes<'tx>,
) -> (u64, Bytes<'tx>, Bytes<'tx>) {
    let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

    let mut k = db_value;
    let v = k.split_off(common::ADDRESS_LENGTH);

    (block_n, k.into(), v)
}

pub fn from_storage_db_format<'tx>(
    db_key: Bytes<'tx>,
    mut db_value: Bytes<'tx>,
) -> (u64, Bytes<'tx>, Bytes<'tx>) {
    let st_sz = common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH;

    let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

    let mut k = vec![0; st_sz];
    let db_key = &db_key[common::BLOCK_NUMBER_LENGTH..]; // remove block_n bytes

    k[..db_key.len()].copy_from_slice(&db_key[..]);
    k[db_key.len()..].copy_from_slice(&db_value[..common::HASH_LENGTH]);

    let v = db_value.split_off(common::HASH_LENGTH);

    (block_n, k.into(), v)
}
