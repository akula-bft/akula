use super::*;
use crate::{common, dbutils, CursorDupSort};
use async_stream::try_stream;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use std::io::Write;

pub fn walk_reverse<C: CursorDupSort>(
    c: &mut C,
    from: u64,
    to: u64,
    key_prefix_len: usize,
) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
    Box::pin(try_stream! {
        let from_db_format = from_db_format(key_prefix_len);

        let _ = c.seek(&dbutils::encode_block_number(to + 1)).await?;
        loop {
            let (k, v) = c.prev().await?;
            if k.is_empty() {
                break;
            }

            let (block_num, k, v) = (from_db_format)(k, v);
            if block_num < from {
                break;
            }

            yield (block_num, k, v);
        }
    })
}

pub fn walk<C: CursorDupSort>(
    c: &mut C,
    from: u64,
    to: u64,
    key_prefix_len: usize,
) -> BoxStream<'_, anyhow::Result<(u64, Bytes, Bytes)>> {
    Box::pin(try_stream! {
        let from_db_format = from_db_format(key_prefix_len);

        let (mut k, mut v) = c.seek(&dbutils::encode_block_number(from)).await?;
        loop {
            if k.is_empty() {
                break;
            }

            let (block_num, k1, v1) = from_db_format(k, v);
            if block_num > to {
                break;
            }

            yield (block_num, k1, v1);

            (k, v) = c.next().await?
        }
    })
}

pub async fn find_in_storage_changeset_2<C: CursorDupSort>(
    c: &mut C,
    block_number: u64,
    key_prefix_len: usize,
    k: &[u8],
) -> anyhow::Result<Option<Bytes>> {
    do_search_2(
        c,
        block_number,
        key_prefix_len,
        &k[..key_prefix_len],
        &k[key_prefix_len + common::INCARNATION_LENGTH
            ..key_prefix_len + common::HASH_LENGTH + common::INCARNATION_LENGTH],
        u64::from_be_bytes(*array_ref!(&k[key_prefix_len..], 0, 8)),
    )
    .await
}

pub async fn find_without_incarnation_in_storage_changeset_2<C: CursorDupSort>(
    c: &mut C,
    block_number: u64,
    key_prefix_len: usize,
    addr_bytes_to_find: &[u8],
    key_bytes_to_find: &[u8],
) -> anyhow::Result<Option<Bytes>> {
    do_search_2(
        c,
        block_number,
        key_prefix_len,
        addr_bytes_to_find,
        key_bytes_to_find,
        0,
    )
    .await
}

pub async fn do_search_2<C: CursorDupSort>(
    c: &mut C,
    block_number: u64,
    key_prefix_len: usize,
    addr_bytes_to_find: &[u8],
    key_bytes_to_find: &[u8],
    incarnation: u64,
) -> anyhow::Result<Option<Bytes>> {
    let from_db_format = from_db_format(key_prefix_len);

    if incarnation == 0 {
        let mut seek = vec![0; 8 + key_prefix_len];
        seek[..].as_mut().write(&block_number.to_be_bytes());
        seek[8..].as_mut().write(addr_bytes_to_find).unwrap();
        let (mut k, mut v) = c.seek(&*seek).await?;
        loop {
            if k.is_empty() {
                break;
            }

            let (_, k1, v1) = (from_db_format)(k, v);
            if !k1.starts_with(addr_bytes_to_find) {
                break;
            }

            let st_hash = &k1[key_prefix_len + common::INCARNATION_LENGTH..];
            if st_hash == key_bytes_to_find {
                return Ok(Some(v1));
            }

            (k, v) = c.next().await?
        }

        return Ok(None);
    }

    let mut seek = vec![0; 8 + key_prefix_len + common::INCARNATION_LENGTH];
    seek[..8].copy_from_slice(&block_number.to_be_bytes());
    seek[8..].as_mut().write(addr_bytes_to_find).unwrap();
    seek[8 + key_prefix_len..].copy_from_slice(&incarnation.to_be_bytes());

    let (k, v) = c.seek_both_range(&seek, key_bytes_to_find).await?;
    if k.is_empty() {
        return Ok(None);
    }

    if !v.starts_with(key_bytes_to_find) {
        return Ok(None);
    }

    let (_, _, v) = (from_db_format)(k, v);

    Ok(Some(v))
}

pub fn encode_storage_2(
    block_n: u64,
    mut s: ChangeSet,
    key_prefix_len: usize,
) -> impl Iterator<Item = (Bytes, Bytes)> {
    s.changes.sort_unstable();

    let key_part = key_prefix_len + common::INCARNATION_LENGTH;

    s.changes.into_iter().map(move |cs| {
        let mut new_k = BytesMut::with_capacity(common::BLOCK_NUMBER_LENGTH + key_part);
        new_k.put_u64(block_n);
        new_k.put_slice(&cs.key[..key_part]);

        let mut new_v = BytesMut::with_capacity(common::HASH_LENGTH + cs.value.len());
        new_v.put_slice(&cs.key[key_part..]);
        new_v.put_slice(&cs.value[..]);

        (new_k.freeze(), new_v.freeze())
    })
}
