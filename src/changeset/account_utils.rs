use super::*;
use crate::{dbutils, CursorDupSort};
use bytes::{Bytes, BytesMut};

pub async fn find_in_account_changeset<C: CursorDupSort>(
    c: &mut C,
    block_number: u64,
    key: &[u8],
) -> anyhow::Result<Option<Bytes>> {
    let (k, v) = c
        .seek_both_range(&dbutils::encode_block_number(block_number), key)
        .await?;

    if k.is_empty() {
        return Ok(None);
    }

    let (_, k, v) = from_account_db_format(k, v);

    if !k.starts_with(key) {
        return Ok(None);
    }

    Ok(Some(v))
}

pub fn encode_accounts<Key: Ord + AsRef<[u8]>>(
    block_number: u64,
    s: &ChangeSet<Key>,
) -> impl Iterator<Item = ([u8; common::BLOCK_NUMBER_LENGTH], Bytes)> + '_ {
    let new_k = dbutils::encode_block_number(block_number);

    s.iter().map(move |cs| {
        let mut new_v = BytesMut::with_capacity(cs.key.as_ref().len() + cs.value.len());
        new_v.extend_from_slice(cs.key.as_ref());
        new_v.extend_from_slice(&*cs.value);

        (new_k, new_v.freeze())
    })
}
