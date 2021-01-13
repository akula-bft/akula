use crate::{common, dbutils::*, Cursor};
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

pub static MAPPER: SyncLazy<HashMap<Bucket, BucketMap>> = SyncLazy::new(|| {
    vec![
        (
            Bucket::AccountChangeSet,
            BucketMap {
                index_bucket: Bucket::AccountsHistory,
                walker_adapter: Box::new(|c| todo!()),
                key_size: common::HASH_LENGTH,
                template: "acc-ind-",
                new: ChangeSet::new_account,
                encode: account::encode_accounts,
                decode: Box::new(from_db_format(common::HASH_LENGTH)),
            },
        ),
        // (Bucket::StorageChangeSet, todo!()),
        (
            Bucket::PlainAccountChangeSet,
            BucketMap {
                index_bucket: Bucket::AccountsHistory,
                walker_adapter: Box::new(|c| todo!()),
                key_size: common::ADDRESS_LENGTH,
                template: "acc-ind-",
                new: ChangeSet::new_account_plain,
                encode: account::encode_accounts_plain,
                decode: Box::new(from_db_format(common::ADDRESS_LENGTH)),
            },
        ),
        // (Bucket::PlainStorageChangeSet, todo!()),
    ]
    .into_iter()
    .collect()
});

pub struct BucketMap {
    pub index_bucket: Bucket,
    pub walker_adapter: Box<
        dyn Fn(&dyn Cursor) -> BoxStream<'static, anyhow::Result<(Bytes, Bytes)>> + Send + Sync,
    >,
    pub key_size: usize,
    pub template: &'static str,
    pub new: fn() -> ChangeSet,
    pub encode: fn(u64, s: ChangeSet) -> Box<dyn Iterator<Item = (Bytes, Bytes)> + Send>,
    pub decode: Box<dyn Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) + Send + Sync>,
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
