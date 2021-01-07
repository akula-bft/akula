use crate::{dbutils::*, Cursor};
use arrayref::array_ref;
use bytes::{Buf, Bytes};
use ethereum_types::H256;
use futures::stream::BoxStream;
use maplit::hashmap;
use std::{collections::HashMap, lazy::SyncLazy, mem::size_of};

mod account;

pub static MAPPER: SyncLazy<HashMap<Bucket, BucketMap>> = SyncLazy::new(|| {
    vec![
        (
            Bucket::AccountChangeSet,
            BucketMap {
                index_bucket: Bucket::AccountsHistory,
                walker_adapter: Box::new(|cursor| todo!()),
                key_size: H256::len_bytes(),
                template: "acc-ind-",
                new: ChangeSet::new_account,
                encode: account::encode_accounts,
                decode: Box::new(from_db_format(H256::len_bytes())),
            },
        ),
        (Bucket::StorageChangeSet, todo!()),
        (Bucket::PlainAccountChangeSet, todo!()),
        (Bucket::PlainStorageChangeSet, todo!()),
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
    pub encode: fn(
        u64,
        s: &ChangeSet,
        f: Box<dyn Fn(Bytes, Bytes) -> anyhow::Result<()> + Send + Sync>,
    ) -> anyhow::Result<()>,
    pub decode: Box<dyn Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) + Send + Sync>,
}

#[derive(Clone, Debug)]
pub struct Change {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Clone, Debug)]
pub struct ChangeSet {
    pub changes: Vec<Change>,
    key_len: usize,
}

pub fn from_db_format(addr_size: usize) -> impl Fn(Bytes, Bytes) -> (u64, Bytes, Bytes) {
    const BLOCK_N_SIZE: usize = 8;

    let st_sz = addr_size + size_of::<Incarnation>() + H256::len_bytes();

    move |mut db_key, mut db_value| {
        let block_n = u64::from_be_bytes(*array_ref!(db_key, 0, size_of::<u64>()));

        let k: Bytes;
        let v: Bytes;
        if db_key.len() == 8 {
            k = db_value.clone();
            v = db_value.split_off(addr_size);
        } else {
            let mut k_tmp = vec![0_u8; st_sz];
            db_key.advance(BLOCK_N_SIZE); // remove block_n bytes

            k_tmp[..db_key.len()].copy_from_slice(&db_key[..]);
            k_tmp[db_key.len()..].copy_from_slice(&db_value[..H256::len_bytes()]);

            k = k_tmp.into();
            v = db_value.split_off(H256::len_bytes());
        }

        (block_n, k, v)
    }
}
