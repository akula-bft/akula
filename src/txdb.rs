use crate::{dbutils::*, kv::*, Cursor};
use async_stream::try_stream;
use bytes::Bytes;
use tokio_stream::Stream;

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

pub fn walk<'tx: 'cur, 'cur, T, C>(
    c: &'cur mut C,
    start_key: &'cur [u8],
    fixed_bits: u64,
) -> impl Stream<Item = anyhow::Result<(Bytes<'tx>, Bytes<'tx>)>> + 'cur
where
    T: Table,
    C: Cursor<'tx, T>,
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

/// Like `walk`, but satisfies borrowck where it complains
pub async fn get_n<'tx: 'cur, 'cur, T, C>(
    c: &'cur mut C,
    start_key: &'cur [u8],
    fixed_bits: u64,
    n: usize,
) -> anyhow::Result<Vec<(Bytes<'tx>, Bytes<'tx>)>>
where
    T: Table,
    C: Cursor<'tx, T>,
{
    let mut out = Vec::with_capacity(n);

    let (fixed_bytes, mask) = bytes_mask(fixed_bits);

    if let Some((mut k, mut v)) = c.seek(start_key).await? {
        while walk_continue(&k, fixed_bytes, fixed_bits, &start_key, mask) {
            if out.len() == n {
                break;
            }

            out.push((k, v));

            match c.next().await? {
                Some((k1, v1)) => {
                    (k, v) = (k1, v1);
                }
                None => break,
            }
        }
    }

    Ok(out)
}
