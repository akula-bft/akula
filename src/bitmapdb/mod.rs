use crate::{
    kv::{tables::BitmapKey, traits::*},
    models::*,
};
use croaring::{treemap::NativeSerializer, Treemap as RoaringTreemap};
use std::iter::Peekable;
use tokio::pin;
use tokio_stream::StreamExt;

// Size beyond which we get MDBX overflow pages: 4096 / 2 - (key_size + 8)
pub const CHUNK_LIMIT: usize = 1950;

pub async fn get<'db, Tx, T, K>(
    tx: &Tx,
    table: T,
    key: K,
    from: impl Into<BlockNumber>,
    to: impl Into<BlockNumber>,
) -> anyhow::Result<RoaringTreemap>
where
    Tx: Transaction<'db>,
    K: Clone + PartialEq + Send,
    BitmapKey<K>: TableDecode,
    T: Table<Key = BitmapKey<K>, Value = RoaringTreemap, SeekKey = BitmapKey<K>>,
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

    pin!(s);

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

    let sz = bm.get_serialized_size_in_bytes();
    if sz <= size_limit {
        let v = std::mem::replace(bm, RoaringTreemap::create());

        return Some(v);
    }

    let mut v = RoaringTreemap::create();

    let mut it = bm.iter().peekable();

    let mut min_n = None;
    while let Some(n) = it.peek() {
        v.add(*n);
        if v.get_serialized_size_in_bytes() > size_limit {
            v.remove(*n);
            min_n = Some(*n);
            break;
        }
        it.next();
    }

    drop(it);

    if let Some(min_n) = min_n {
        let to_remove = bm.iter().take_while(|&n| n < min_n).collect::<Vec<_>>();
        for element in to_remove {
            bm.remove(element);
        }
        Some(v)
    } else {
        Some(std::mem::replace(bm, RoaringTreemap::create()))
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
                        chunk.maximum().unwrap()
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
            let mut bm = RoaringTreemap::create();

            for j in (0..10_000).filter(|j| j % 20 == 0) {
                for e in j..j + 10 {
                    bm.add(e);
                }
            }

            while !bm.is_empty() {
                let lft = super::cut_left(&mut bm, n).unwrap();
                let lft_size = lft.get_serialized_size_in_bytes();
                if !bm.is_empty() {
                    assert!(lft_size > n - 256 && lft_size < n + 256);
                } else {
                    assert!(lft.get_serialized_size_in_bytes() > 0);
                    assert!(lft_size < n + 256);
                }
            }
        }

        const N: usize = 2048;
        {
            let mut bm = RoaringTreemap::create();
            bm.add(1);
            let lft = super::cut_left(&mut bm, N).unwrap();
            assert!(lft.get_serialized_size_in_bytes() > 0);
            assert_eq!(lft.cardinality(), 1);
            assert_eq!(bm.cardinality(), 0);
        }

        {
            let mut bm = RoaringTreemap::create();
            assert_eq!(super::cut_left(&mut bm, N), None);
            assert_eq!(bm.cardinality(), 0);
        }
    }
}
