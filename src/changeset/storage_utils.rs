use super::*;
use crate::{common, CursorDupSort};
use bytes::Bytes;
use std::io::Write;

pub async fn find_in_storage_changeset_2<'tx, C>(
    c: &mut C,
    block_number: u64,
    k: &[u8],
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    do_search_2(
        c,
        block_number,
        &k[..common::ADDRESS_LENGTH],
        &k[common::ADDRESS_LENGTH + common::INCARNATION_LENGTH
            ..common::ADDRESS_LENGTH + common::INCARNATION_LENGTH + common::HASH_LENGTH],
        u64::from_be_bytes(*array_ref!(&k[common::ADDRESS_LENGTH..], 0, 8)),
    )
    .await
}

pub async fn find_without_incarnation_in_storage_changeset_2<'tx, C>(
    c: &mut C,
    block_number: u64,
    addr_bytes_to_find: &[u8],
    key_bytes_to_find: &[u8],
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    do_search_2(c, block_number, addr_bytes_to_find, key_bytes_to_find, 0).await
}

pub async fn do_search_2<'tx, C>(
    c: &mut C,
    block_number: u64,
    addr_bytes_to_find: &[u8],
    key_bytes_to_find: &[u8],
    incarnation: u64,
) -> anyhow::Result<Option<Bytes<'tx>>>
where
    C: CursorDupSort<'tx, tables::StorageChangeSet>,
{
    if incarnation == 0 {
        let mut seek = vec![0; common::BLOCK_NUMBER_LENGTH + common::ADDRESS_LENGTH];
        seek[..]
            .as_mut()
            .write(&block_number.to_be_bytes())
            .unwrap();
        seek[8..].as_mut().write(addr_bytes_to_find).unwrap();
        let mut b = c.seek(&*seek).await?;
        while let Some((k, v)) = b {
            let (_, k1, v1) = from_storage_db_format(k, v);
            if !k1.starts_with(addr_bytes_to_find) {
                break;
            }

            let st_hash = &k1[common::ADDRESS_LENGTH + common::INCARNATION_LENGTH..];
            if st_hash == key_bytes_to_find {
                return Ok(Some(v1));
            }

            b = c.next().await?
        }

        return Ok(None);
    }

    let mut seek =
        vec![0; common::BLOCK_NUMBER_LENGTH + common::ADDRESS_LENGTH + common::INCARNATION_LENGTH];
    seek[..common::BLOCK_NUMBER_LENGTH].copy_from_slice(&block_number.to_be_bytes());
    seek[common::BLOCK_NUMBER_LENGTH..]
        .as_mut()
        .write(addr_bytes_to_find)
        .unwrap();
    seek[common::BLOCK_NUMBER_LENGTH + common::ADDRESS_LENGTH..]
        .copy_from_slice(&incarnation.to_be_bytes());

    if let Some(v) = c.seek_both_range(&seek, key_bytes_to_find).await? {
        if v.starts_with(key_bytes_to_find) {
            let (_, _, v) = from_storage_db_format(seek.into(), v);

            return Ok(Some(v));
        }
    }

    Ok(None)
}
