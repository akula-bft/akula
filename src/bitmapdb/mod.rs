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

    let s = walk(
        &mut c,
        Some(BitmapKey {
            inner: key.clone(),
            block_number: from,
        }),
    )
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

pub struct Chunks {
    bm: RoaringTreemap,
    size_limit: usize,
}

impl Iterator for Chunks {
    type Item = RoaringTreemap;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bm.is_empty() {
            return None;
        }

        let sz = self.bm.get_serialized_size_in_bytes();
        if sz <= self.size_limit {
            return Some(std::mem::replace(&mut self.bm, RoaringTreemap::create()));
        }

        let mut v = RoaringTreemap::create();

        let mut it = self.bm.iter().peekable();

        let mut min_n = None;
        while let Some(n) = it.peek() {
            v.add(*n);
            if v.get_serialized_size_in_bytes() > self.size_limit {
                v.remove(*n);
                min_n = Some(*n);
                break;
            }
            it.next();
        }

        drop(it);

        if let Some(min_n) = min_n {
            let to_remove = self
                .bm
                .iter()
                .take_while(|&n| n < min_n)
                .collect::<Vec<_>>();
            for element in to_remove {
                self.bm.remove(element);
            }
            Some(v)
        } else {
            Some(std::mem::replace(&mut self.bm, RoaringTreemap::create()))
        }
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
    fn chunks() {
        for &n in &[1024, 2048] {
            let mut bm = RoaringTreemap::create();

            for j in (0..10_000).filter(|j| j % 20 == 0) {
                for e in j..j + 10 {
                    bm.add(e);
                }
            }

            let mut iter = Chunks::new(bm, n).peekable();
            while let Some(lft) = iter.next() {
                let lft_size = lft.get_serialized_size_in_bytes();
                if iter.peek().is_some() {
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
            let mut iter = Chunks::new(bm, N);

            let v = iter.next().unwrap();
            assert_eq!(v.cardinality(), 1);
            assert!(v.get_serialized_size_in_bytes() > 0);

            assert_eq!(iter.next(), None);
        }

        assert_eq!(Chunks::new(RoaringTreemap::create(), N).next(), None);
    }
}
