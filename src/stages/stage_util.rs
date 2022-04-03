use crate::{
    bitmapdb::{self, CHUNK_LIMIT},
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, BitmapKey},
        traits::*,
    },
    models::*,
};
use anyhow::format_err;
use itertools::Itertools;
use std::collections::{BTreeSet, HashMap};

pub fn should_do_clean_promotion<'db, 'tx, K, E>(
    tx: &'tx MdbxTransaction<'db, K, E>,
    genesis: BlockNumber,
    past_progress: BlockNumber,
    max_block: BlockNumber,
    threshold: u64,
) -> anyhow::Result<bool>
where
    'db: 'tx,
    K: TransactionKind,
    E: EnvironmentKind,
{
    let current_gas = tx
        .get(tables::TotalGas, past_progress)?
        .ok_or_else(|| format_err!("No cumulative index for block {}", past_progress))?;
    let max_gas = tx
        .get(tables::TotalGas, max_block)?
        .ok_or_else(|| format_err!("No cumulative index for block {}", max_block))?;

    let gas_progress = max_gas.checked_sub(current_gas).ok_or_else(|| {
        format_err!(
            "Faulty cumulative index: max gas less than current gas ({} < {})",
            max_gas,
            current_gas
        )
    })?;

    Ok(past_progress == genesis || gas_progress > threshold)
}

pub fn load_bitmap<T, K>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    mut collector: Collector<'_, K, croaring::Treemap>,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<K>, Value = croaring::Treemap>,
    K: TableObject + PartialEq + Copy,
    <K as TableEncode>::Encoded: Ord,
    BitmapKey<K>: TableDecode,
    Vec<u8>: From<<K as TableEncode>::Encoded>,
{
    for res in collector
        .iter()
        .map(|res| {
            let (k, bitmap) = res?;

            let k = K::decode(&k)?;
            let bitmap = croaring::Treemap::decode(&bitmap)?;

            Ok::<_, anyhow::Error>((k, bitmap))
        })
        .coalesce(|prev, current| match (prev, current) {
            (Ok((prev_k, prev_bitmap)), Ok((current_k, current_bitmap))) => {
                if prev_k == current_k {
                    Ok(Ok((prev_k, prev_bitmap | current_bitmap)))
                } else {
                    Err((Ok((prev_k, prev_bitmap)), Ok((current_k, current_bitmap))))
                }
            }
            err => Err(err),
        })
    {
        let (k, mut total_bitmap) = res?;

        if !total_bitmap.is_empty() {
            if let Some((_, last_bitmap)) = cursor.seek_exact(BitmapKey {
                inner: k,
                block_number: BlockNumber(u64::MAX),
            })? {
                total_bitmap |= last_bitmap;
            }

            for (block_number, bitmap) in
                bitmapdb::Chunks::new(total_bitmap, CHUNK_LIMIT).with_keys()
            {
                cursor.put(
                    BitmapKey {
                        inner: k,
                        block_number,
                    },
                    bitmap,
                )?;
            }
        }
    }

    Ok(())
}

pub fn unwind_bitmap<T, K>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    keys: BTreeSet<K>,
    unwind_to: BlockNumber,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<K>, Value = croaring::Treemap>,
    K: PartialEq + Copy,
    BitmapKey<K>: TableDecode,
{
    for key in keys {
        let mut bm = cursor
            .seek_exact(BitmapKey {
                inner: key,
                block_number: BlockNumber(u64::MAX),
            })?
            .map(|(_, bm)| bm);

        while let Some(b) = bm {
            cursor.delete_current()?;

            let new_bm = b
                .iter()
                .take_while(|&v| v <= *unwind_to)
                .collect::<croaring::Treemap>();

            if new_bm.cardinality() > 0 {
                cursor.upsert(
                    BitmapKey {
                        inner: key,
                        block_number: BlockNumber(u64::MAX),
                    },
                    new_bm,
                )?;
            }

            bm = cursor
                .prev()?
                .and_then(|(BitmapKey { inner, .. }, b)| if inner == key { Some(b) } else { None });
        }
    }

    Ok(())
}

pub fn prune_bitmap<T, K>(
    cursor: &mut MdbxCursor<'_, RW, T>,
    keys: BTreeSet<K>,
    prune_to: BlockNumber,
) -> anyhow::Result<()>
where
    T: Table<Key = BitmapKey<K>, Value = croaring::Treemap, SeekKey = BitmapKey<K>>,
    K: PartialEq + Copy,
    BitmapKey<K>: TableDecode,
{
    for key in keys {
        let mut bm = cursor.seek(BitmapKey {
            inner: key,
            block_number: BlockNumber(0),
        })?;

        while let Some((
            BitmapKey {
                inner,
                block_number,
            },
            b,
        )) = bm
        {
            if inner != key {
                break;
            }

            cursor.delete_current()?;

            if block_number >= prune_to {
                let new_bm = b
                    .iter()
                    .skip_while(|&v| v < *prune_to)
                    .collect::<croaring::Treemap>();

                if new_bm.cardinality() > 0 {
                    cursor.upsert(
                        BitmapKey {
                            inner: key,
                            block_number,
                        },
                        new_bm,
                    )?;
                }

                break;
            }

            bm = cursor.next()?;
        }
    }

    Ok(())
}

pub fn flush_bitmap<K>(
    collector: &mut Collector<K, croaring::Treemap>,
    src: &mut HashMap<K, croaring::Treemap>,
) where
    K: TableEncode,
    <K as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<K as TableEncode>::Encoded>,
{
    for (address, index) in src.drain() {
        collector.push(address, index);
    }
}
