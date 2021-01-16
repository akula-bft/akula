use super::{from_db_format, ChangeSet};
use crate::{dbutils, CursorDupSort};
use bytes::{Bytes, BytesMut};

pub async fn find_in_account_changeset<C: CursorDupSort>(
    c: &mut C,
    block_number: u64,
    key: &[u8],
    key_len: usize,
) -> anyhow::Result<Option<Bytes>> {
    let from_db_format = from_db_format(key_len);

    let (k, v) = c
        .seek_both_range(&dbutils::encode_block_number(block_number), key)
        .await?;

    if k.is_empty() {
        return Ok(None);
    }

    let (_, k, v) = from_db_format(k, v);

    if !k.starts_with(key) {
        return Ok(None);
    }

    Ok(Some(v))
}

pub fn encode_accounts(
    block_number: u64,
    mut s: ChangeSet,
) -> impl Iterator<Item = (Bytes, Bytes)> {
    s.changes.sort_unstable();

    let new_k = Bytes::copy_from_slice(&dbutils::encode_block_number(block_number));

    s.changes.into_iter().map(move |cs| {
        let mut new_v = BytesMut::with_capacity(cs.key.len() + cs.value.len());
        new_v.extend_from_slice(&*cs.key);
        new_v.extend_from_slice(&*cs.value);

        (new_k.clone(), new_v.freeze())
    })
}
