use crate::{
    bitmapdb::{self, CHUNK_LIMIT},
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, BitmapKey},
        traits::*,
    },
    models::*,
    stagedsync::stage::{ExecOutput, PruningInput, StageInput, UnwindInput, UnwindOutput},
};
use anyhow::format_err;
use croaring::Treemap;
use itertools::Itertools;
use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::pin;
use tracing::*;

pub(crate) fn should_do_clean_promotion<'db, 'tx, K, E>(
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

pub(crate) fn load_bitmap<T, K>(
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

pub(crate) fn unwind_bitmap<T, K>(
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
                break;
            }

            bm = cursor
                .prev()?
                .and_then(|(BitmapKey { inner, .. }, b)| if inner == key { Some(b) } else { None });
        }
    }

    Ok(())
}

pub(crate) fn prune_bitmap<T, K>(
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

pub(crate) fn flush_bitmap<K>(
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

#[derive(Clone, Debug)]
pub struct IndexParams {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

pub(crate) fn execute_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
    tx: &mut MdbxTransaction<'_, RW, E>,
    input: StageInput,
    IndexParams {
        temp_dir,
        flush_interval,
    }: &IndexParams,
    data_table: DataTable,
    index_table: IndexTable,
    extractor: Extractor,
) -> anyhow::Result<ExecOutput>
where
    E: EnvironmentKind,
    DataKey: TableDecode,
    DataTable: Table<Key = DataKey, Value = DataValue, SeekKey = BlockNumber>,
    IndexKey: Hash + Ord + Copy + TableObject,
    BitmapKey<IndexKey>: TableDecode,
    <IndexKey as TableEncode>::Encoded: Ord,
    Vec<u8>: From<<IndexKey as TableEncode>::Encoded>,
    IndexTable: Table<Key = BitmapKey<IndexKey>, Value = Treemap, SeekKey = BitmapKey<IndexKey>>,
    Extractor: Fn(DataKey, DataValue) -> (BlockNumber, IndexKey),
{
    let starting_block = input.stage_progress.unwrap_or(BlockNumber(0));
    let max_block = input
        .previous_stage
        .ok_or_else(|| format_err!("Index generation cannot be the first stage"))?
        .1;

    let walker = tx
        .cursor(data_table)?
        .walk(input.stage_progress.map(|x| x + 1));
    pin!(walker);

    let mut keys = HashMap::<IndexKey, croaring::Treemap>::new();

    let mut collector =
        Collector::<IndexKey, croaring::Treemap>::new(temp_dir, OPTIMAL_BUFFER_CAPACITY);

    let mut highest_block = starting_block;
    let mut last_flush = starting_block;

    let mut printed = false;
    let mut last_log = Instant::now();
    while let Some((data_key, data_value)) = walker.next().transpose()? {
        let (block_number, index_key) = (extractor)(data_key, data_value);

        if block_number > max_block {
            break;
        }

        keys.entry(index_key).or_default().add(block_number.0);

        if highest_block != block_number {
            highest_block = block_number;

            if highest_block.0 - last_flush.0 >= *flush_interval {
                flush_bitmap(&mut collector, &mut keys);

                last_flush = highest_block;
            }
        }

        let now = Instant::now();
        if last_log - now > Duration::from_secs(30) {
            info!("Current block: {}", block_number);
            printed = true;
            last_log = now;
        }
    }

    flush_bitmap(&mut collector, &mut keys);

    if printed {
        info!("Flushing index");
    }
    load_bitmap(&mut tx.cursor(index_table)?, collector)?;

    Ok(ExecOutput::Progress {
        stage_progress: max_block,
        done: true,
        reached_tip: true,
    })
}

pub(crate) fn unwind_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
    tx: &mut MdbxTransaction<'_, RW, E>,
    input: UnwindInput,
    data_table: DataTable,
    index_table: IndexTable,
    extractor: Extractor,
) -> anyhow::Result<UnwindOutput>
where
    E: EnvironmentKind,
    DataKey: TableDecode,
    DataTable: Table<Key = DataKey, Value = DataValue, SeekKey = BlockNumber>,
    IndexKey: Ord + Copy,
    BitmapKey<IndexKey>: TableDecode,
    IndexTable: Table<Key = BitmapKey<IndexKey>, Value = Treemap, SeekKey = BitmapKey<IndexKey>>,
    Extractor: Fn(DataKey, DataValue) -> IndexKey,
{
    let walker = tx.cursor(data_table)?.walk(Some(input.unwind_to + 1));
    pin!(walker);

    let mut keys = BTreeSet::new();
    while let Some((key, value)) = walker.next().transpose()? {
        keys.insert((extractor)(key, value));
    }

    unwind_bitmap(&mut tx.cursor(index_table)?, keys, input.unwind_to)?;

    Ok(UnwindOutput {
        stage_progress: input.unwind_to,
    })
}

pub(crate) fn prune_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
    tx: &mut MdbxTransaction<'_, RW, E>,
    input: PruningInput,
    data_table: DataTable,
    index_table: IndexTable,
    extractor: Extractor,
) -> anyhow::Result<()>
where
    E: EnvironmentKind,
    DataKey: TableDecode,
    DataTable: Table<Key = DataKey, Value = DataValue>,
    IndexKey: Ord + Copy,
    BitmapKey<IndexKey>: TableDecode,
    IndexTable: Table<Key = BitmapKey<IndexKey>, Value = Treemap, SeekKey = BitmapKey<IndexKey>>,
    Extractor: Fn(DataKey, DataValue) -> (BlockNumber, IndexKey),
{
    let walker = tx.cursor(data_table)?.walk(None);
    pin!(walker);

    let mut keys = BTreeSet::new();
    while let Some((key, value)) = walker.next().transpose()? {
        let (block_number, index_key) = (extractor)(key, value);

        if block_number >= input.prune_to {
            break;
        }

        keys.insert(index_key);
    }

    prune_bitmap(&mut tx.cursor(index_table)?, keys, input.prune_to)?;

    Ok(())
}
