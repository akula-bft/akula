use std::iter::Peekable;

use crate::{common, kv::Table, txdb, Transaction};
use arrayref::array_ref;
use pin_utils::pin_mut;
use roaring::RoaringTreemap;
use tokio_stream::StreamExt;

// Size beyond which we get MDBX overflow pages: 4096 / 2 - (key_size + 8)
pub const CHUNK_LIMIT: usize = 1950;

pub async fn get_64<'db, Tx, T>(
    tx: &Tx,
    table: &T,
    key: &[u8],
    from: u64,
    to: u64,
) -> anyhow::Result<RoaringTreemap>
where
    Tx: Transaction<'db>,
    T: Table,
{
    let mut out: Option<RoaringTreemap> = None;

    let from_key = key
        .iter()
        .chain(&from.to_be_bytes())
        .copied()
        .collect::<Vec<_>>();

    let mut c = tx.cursor(table).await?;

    let s = txdb::walk(&mut c, &from_key, (key.len() * 8) as u64);

    pin_mut!(s);

    while let Some((k, v)) = s.try_next().await? {
        let v = RoaringTreemap::deserialize_from(v.as_ref())?;

        if out.is_some() {
            out = Some(out.unwrap() | v);
        } else {
            out = Some(v);
        }

        if u64::from_be_bytes(*array_ref!(
            k[k.len() - common::BLOCK_NUMBER_LENGTH..],
            0,
            common::BLOCK_NUMBER_LENGTH
        )) >= to
        {
            break;
        }
    }

    Ok(out.unwrap_or_default())
}

fn cut_left_64(bm: &mut RoaringTreemap, size_limit: usize) -> Option<RoaringTreemap> {
    if bm.is_empty() {
        return None;
    }

    let sz = bm.serialized_size();
    if sz <= size_limit {
        let v = std::mem::replace(bm, RoaringTreemap::new());

        return Some(v);
    }

    let from = bm.min().unwrap();
    let min_max = bm.max().unwrap() - bm.min().unwrap();

    let mut v = RoaringTreemap::new();

    let mut it = bm.iter().peekable();
    while let Some(n) = it.peek() {
        v.push(*n);
        if v.serialized_size() > size_limit {
            v.remove(*n);
            break;
        }
        it.next();
    }

    Some(v)
}

pub struct Chunks {
    bm: RoaringTreemap,
    size_limit: usize,
}

impl Iterator for Chunks {
    type Item = RoaringTreemap;

    fn next(&mut self) -> Option<Self::Item> {
        cut_left_64(&mut self.bm, self.size_limit)
    }
}

impl Chunks {
    pub fn new(bm: RoaringTreemap, size_limit: usize) -> Self {
        Self { bm, size_limit }
    }

    pub fn with_keys<'a>(self, k: &'a [u8]) -> ChunkWithKeys<'a> {
        ChunkWithKeys {
            inner: self.peekable(),
            k,
        }
    }
}

pub struct ChunkWithKeys<'a> {
    inner: Peekable<Chunks>,
    k: &'a [u8],
}

impl<'a> Iterator for ChunkWithKeys<'a> {
    type Item = (Vec<u8>, RoaringTreemap);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|chunk| {
            let mut chunk_key = self
                .k
                .iter()
                .chain(
                    &if self.inner.peek().is_none() {
                        u64::MAX ^ 0
                    } else {
                        chunk.max().unwrap()
                    }
                    .to_be_bytes(),
                )
                .copied()
                .collect();

            (chunk_key, chunk)
        })
    }
}
