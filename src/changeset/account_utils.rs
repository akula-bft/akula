use super::*;
use crate::{dbutils, CursorDupSort};
use bytes::Bytes;

pub async fn find_in_account_changeset<
    'tx,
    C: CursorDupSort<'tx, buckets::PlainAccountChangeSet>,
>(
    c: &mut C,
    block_number: u64,
    key: &[u8],
) -> anyhow::Result<Option<Bytes<'static>>> {
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
