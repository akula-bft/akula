use super::*;
use crate::{dbutils, CursorDupSort};
use bytes::Bytes;

pub async fn find_in_account_changeset<'tx, Txn, C>(
    c: &mut C,
    block_number: u64,
    key: &[u8],
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    Txn: Transaction<'tx>,
    C: CursorDupSort<'tx, Txn, tables::PlainAccountChangeSet>,
{
    let k = dbutils::encode_block_number(block_number);
    if let Some(v) = c.seek_both_range(&k, key).await? {
        let (_, k, v) = from_account_db_format(k.to_vec().into(), v);

        if k.starts_with(key) {
            return Ok(Some(v));
        }
    }

    Ok(None)
}
