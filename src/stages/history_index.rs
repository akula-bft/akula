use crate::{
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, AccountChange, BitmapKey, StorageChange, StorageChangeKey},
        traits::*,
    },
    models::*,
    stagedsync::{stage::*, stages::*},
    stages::stage_util::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use croaring::Treemap;
use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use tokio::pin;
use tracing::info;

/// Generate account history index
#[derive(Debug)]
pub struct AccountHistoryIndex {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for AccountHistoryIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        ACCOUNT_HISTORY_INDEX
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        execute_index(
            tx,
            input,
            &*self.temp_dir,
            self.flush_interval,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |block_number, AccountChange { address, .. }| (block_number, address),
        )
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        unwind_index(
            tx,
            input,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |_, AccountChange { address, .. }| address,
        )
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        prune_index(
            tx,
            input,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |block_number, AccountChange { address, .. }| (block_number, address),
        )
    }
}

/// Generate storage history index
#[derive(Debug)]
pub struct StorageHistoryIndex {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

#[async_trait]
impl<'db, E> Stage<'db, E> for StorageHistoryIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        STORAGE_HISTORY_INDEX
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        execute_index(
            tx,
            input,
            &*self.temp_dir,
            self.flush_interval,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey {
                 block_number,
                 address,
             },
             StorageChange { location, .. }| (block_number, (address, location)),
        )
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        unwind_index(
            tx,
            input,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey { address, .. }, StorageChange { location, .. }| (address, location),
        )
    }

    async fn prune<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: PruningInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        prune_index(
            tx,
            input,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey {
                 block_number,
                 address,
             },
             StorageChange { location, .. }| (block_number, (address, location)),
        )
    }
}

fn execute_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
    tx: &mut MdbxTransaction<'_, RW, E>,
    input: StageInput,
    temp_dir: &TempDir,
    flush_interval: u64,
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

    let walker = tx.cursor(data_table)?.walk(Some(starting_block + 1));
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

            if highest_block.0 - last_flush.0 >= flush_interval {
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
    })
}

fn unwind_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
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

fn prune_index<E, DataKey, DataValue, DataTable, IndexKey, IndexTable, Extractor>(
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
