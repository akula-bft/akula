use crate::{common, dbutils::*, Cursor, CursorDupSort};
use anyhow::bail;
use arrayref::array_ref;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::{collections::HashMap, lazy::SyncLazy};

mod account;
mod account_utils;
mod storage;
mod storage_utils;
pub use self::{account::*, account_utils::*, storage::*, storage_utils::*};

#[async_trait]
pub trait Walker2: Send {
    fn walk(&mut self, from: u64, to: u64) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>>;
    fn walk_reverse(
        &mut self,
        from: u64,
        to: u64,
    ) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>>;
    async fn find(&mut self, block_number: u64, k: &[u8]) -> anyhow::Result<Option<Bytes>>;
}

pub trait ChangeSetBucket: Bucket {
    const KEY_SIZE: usize;
    const TEMPLATE: &'static str;

    type IndexBucket: Bucket;
    type Walker2<'cur, C: 'cur + CursorDupSort>: Walker2;
    type EncodedStream: EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(cursor: &'cur mut C)
        -> Self::Walker2<'cur, C>;
    fn make_changeset() -> ChangeSet;
    fn encode(block_number: u64, s: ChangeSet) -> Self::EncodedStream;
    fn decode(k: Bytes, v: Bytes) -> (u64, Bytes, Bytes);
}

impl ChangeSetBucket for buckets::AccountChangeSet {
    const KEY_SIZE: usize = common::HASH_LENGTH;
    const TEMPLATE: &'static str = "acc-ind-";

    type IndexBucket = buckets::AccountsHistory;
    type Walker2<'cur, C: 'cur + CursorDupSort> = impl Walker2;
    type EncodedStream = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker2<'cur, C> {
        AccountChangeSet { c }
    }

    fn make_changeset() -> ChangeSet {
        ChangeSet::new_account()
    }

    fn encode(block_number: u64, s: ChangeSet) -> Self::EncodedStream {
        encode_accounts(block_number, s)
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Bytes, Bytes) {
        from_db_format(common::HASH_LENGTH)(k, v)
    }
}

impl ChangeSetBucket for buckets::PlainAccountChangeSet {
    const KEY_SIZE: usize = common::ADDRESS_LENGTH;
    const TEMPLATE: &'static str = "acc-ind-";

    type IndexBucket = buckets::AccountsHistory;
    type Walker2<'cur, C: 'cur + CursorDupSort> = impl Walker2;
    type EncodedStream = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker2<'cur, C> {
        AccountChangeSetPlain { c }
    }

    fn make_changeset() -> ChangeSet {
        ChangeSet::new_account_plain()
    }

    fn encode(block_number: u64, s: ChangeSet) -> Self::EncodedStream {
        encode_accounts(block_number, s)
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Bytes, Bytes) {
        from_db_format(common::ADDRESS_LENGTH)(k, v)
    }
}

impl ChangeSetBucket for buckets::StorageChangeSet {
    const KEY_SIZE: usize = common::HASH_LENGTH;
    const TEMPLATE: &'static str = "st-ind-";

    type IndexBucket = buckets::StorageHistory;
    type Walker2<'cur, C: 'cur + CursorDupSort> = impl Walker2;
    type EncodedStream = impl EncodedStream;

    fn walker_adapter<'cur, C: 'cur + CursorDupSort>(c: &'cur mut C) -> Self::Walker2<'cur, C> {
        StorageChangeSetPlain { c }
    }

    fn make_changeset() -> ChangeSet {
        ChangeSet::new_storage()
    }

    fn encode(block_number: u64, s: ChangeSet) -> Self::EncodedStream {
        encode_storage(block_number, s)
    }

    fn decode(k: Bytes, v: Bytes) -> (u64, Bytes, Bytes) {
        from_db_format(common::HASH_LENGTH)(k, v)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Change {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangeSet {
    pub changes: Vec<Change>,
    key_len: usize,
}

impl ChangeSet {
    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn key_size(&self) -> usize {
        if self.key_len != 0 {
            return self.key_len;
        }

        self.changes.first().map(|c| c.key.len()).unwrap_or(0)
    }

    fn check_key_size(&self, key: &[u8]) -> anyhow::Result<()> {
        if (self.is_empty() && self.key_size() == 0)
            || (key.len() == self.key_size() && !key.is_empty())
        {
            Ok(())
        } else {
            bail!(
                "wrong key size in AccountChangeSet: expected {}, actual {}",
                self.key_size(),
                key.len()
            )
        }
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes) -> anyhow::Result<()> {
        self.check_key_size(&key)?;

        self.changes.push(Change { key, value });

        Ok(())
    }
}

pub fn from_db_format(addr_size: usize) -> impl Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) {
    const BLOCK_N_SIZE: usize = 8;

    let st_sz = addr_size + common::INCARNATION_LENGTH + common::HASH_LENGTH;

    move |db_key, mut db_value| {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, common::BLOCK_NUMBER_LENGTH));

        let mut k: Bytes;
        let v: Bytes;
        if db_key.len() == 8 {
            k = db_value;
            v = k.split_off(addr_size);
        } else {
            let mut k_tmp = vec![0; st_sz];
            let db_key = &db_key[BLOCK_N_SIZE..]; // remove block_n bytes

            k_tmp[..db_key.len()].copy_from_slice(&db_key[..]);
            k_tmp[db_key.len()..].copy_from_slice(&db_value[..common::HASH_LENGTH]);

            k = k_tmp.into();
            v = db_value.split_off(common::HASH_LENGTH);
        }

        (block_n, k, v)
    }
}
