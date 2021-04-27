use crate::{changeset::*, common, dbutils::*, models::*, Cursor, Transaction};
use anyhow::{bail, Context};
use arrayref::array_ref;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use ethereum::Header;
use ethereum_types::{Address, H256, U256};
use std::collections::{HashMap, HashSet};
use tokio::pin;
use tokio_stream::Stream;
use tracing::*;

fn walk_continue<K: AsRef<[u8]>>(
    k: &[u8],
    fixed_bytes: u64,
    fixed_bits: u64,
    start_key: &K,
    mask: u8,
) -> bool {
    !k.is_empty()
        && k.len() as u64 >= fixed_bytes
        && (fixed_bits == 0
            || (k[..fixed_bytes as usize - 1] == start_key.as_ref()[..fixed_bytes as usize - 1])
                && (k[fixed_bytes as usize - 1] & mask)
                    == (start_key.as_ref()[fixed_bytes as usize - 1] & mask))
}

pub fn walk<'cur, 'tx: 'cur, Txn, B, C>(
    c: &'cur mut C,
    start_key: &'cur [u8],
    fixed_bits: u64,
) -> impl Stream<Item = anyhow::Result<(Bytes<'tx>, Bytes<'tx>)>> + 'cur
where
    B: Table,
    Txn: Transaction<'tx>,
    C: Cursor<'tx, Txn, B>,
{
    try_stream! {
        let (fixed_bytes, mask) = bytes_mask(fixed_bits);

        if let Some((mut k, mut v)) = c.seek(start_key).await? {
            while walk_continue(&k, fixed_bytes, fixed_bits, &start_key, mask) {
                yield (k, v);

                match c.next().await? {
                    Some((k1, v1)) => {
                        (k, v) = (k1, v1);
                    }
                    None => break,
                }
            }
        }
    }
}
