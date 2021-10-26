use crate::{
    kv::{tables::BitmapKey, traits::ttw, Table, TableDecode},
    models::*,
    Cursor, Transaction,
};
use pin_utils::pin_mut;
use roaring::RoaringTreemap;
use std::iter::Peekable;
use tokio_stream::StreamExt;

// Size beyond which we get MDBX overflow pages: 4096 / 2 - (key_size + 8)
pub const CHUNK_LIMIT: usize = 1950;

pub async fn get<'db, Tx, T, K>(
    tx: &Tx,
    table: &T,
    key: K,
    from: impl Into<BlockNumber>,
    to: impl Into<BlockNumber>,
) -> anyhow::Result<RoaringTreemap>
where
    Tx: Transaction<'db>,
    K: Clone + PartialEq + Send,
    BitmapKey<K>: TableDecode,
    T: Table<
        Key = BitmapKey<K>,
        Value = RoaringTreemap,
        FusedValue = (BitmapKey<K>, RoaringTreemap),
        SeekKey = BitmapKey<K>,
    >,
{
    let mut out: Option<RoaringTreemap> = None;
    let from = from.into();
    let to = to.into();

    let mut c = tx.cursor(table).await?;

    let s = c
        .walk(Some(BitmapKey {
            inner: key.clone(),
            block_number: from,
        }))
        .take_while(ttw(|(BitmapKey { inner, .. }, _)| *inner == key));

    pin_mut!(s);

    while let Some((BitmapKey { block_number, .. }, v)) = s.try_next().await? {
        if out.is_some() {
            out = Some(out.unwrap() | v);
        } else {
            out = Some(v);
        }

        if block_number >= to {
            break;
        }
    }

    Ok(out.unwrap_or_default())
}

fn cut_left(bm: &mut RoaringTreemap, size_limit: usize) -> Option<RoaringTreemap> {
    if bm.is_empty() {
        return None;
    }

    let sz = bm.serialized_size();
    if sz <= size_limit {
        let v = std::mem::replace(bm, RoaringTreemap::new());

        return Some(v);
    }

    let mut v = RoaringTreemap::new();

    let mut it = bm.iter().peekable();

    let mut min_n = None;
    while let Some(n) = it.peek() {
        v.push(*n);
        if v.serialized_size() > size_limit {
            v.remove(*n);
            min_n = Some(*n);
            break;
        }
        it.next();
    }

    if let Some(n) = min_n {
        bm.remove_range(0..n);
        Some(v)
    } else {
        Some(std::mem::replace(bm, RoaringTreemap::new()))
    }
}

pub struct Chunks {
    bm: RoaringTreemap,
    size_limit: usize,
}

impl Iterator for Chunks {
    type Item = RoaringTreemap;

    fn next(&mut self) -> Option<Self::Item> {
        cut_left(&mut self.bm, self.size_limit)
    }
}

impl Chunks {
    pub fn new(bm: RoaringTreemap, size_limit: usize) -> Self {
        Self { bm, size_limit }
    }

    pub fn with_keys(self) -> ChunksWithKeys {
        ChunksWithKeys {
            inner: self.peekable(),
        }
    }
}

pub struct ChunksWithKeys {
    inner: Peekable<Chunks>,
}

impl Iterator for ChunksWithKeys {
    type Item = (BlockNumber, RoaringTreemap);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|chunk| {
            (
                BlockNumber({
                    if self.inner.peek().is_none() {
                        u64::MAX
                    } else {
                        chunk.max().unwrap()
                    }
                }),
                chunk,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cut_left() {
        for &n in &[1024, 2048] {
            let mut bm = RoaringTreemap::new();

            for j in (0..10_000).filter(|j| j % 20 == 0) {
                bm.append(j..j + 10).unwrap();
            }

            while !bm.is_empty() {
                let lft = super::cut_left(&mut bm, n).unwrap();
                let lft_size = lft.serialized_size();
                if !bm.is_empty() {
                    assert!(lft_size > n - 256 && lft_size < n + 256);
                } else {
                    assert!(lft.serialized_size() > 0);
                    assert!(lft_size < n + 256);
                }
            }
        }

        const N: usize = 2048;
        {
            let mut bm = RoaringTreemap::new();
            bm.push(1);
            let lft = super::cut_left(&mut bm, N).unwrap();
            assert!(lft.serialized_size() > 0);
            assert_eq!(lft.len(), 1);
            assert_eq!(bm.len(), 0);
        }

        {
            let mut bm = RoaringTreemap::new();
            assert_eq!(super::cut_left(&mut bm, N), None);
            assert_eq!(bm.len(), 0);
        }
    }
}
