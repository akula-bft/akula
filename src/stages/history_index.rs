use crate::{
    etl::collector::*,
    kv::{
        mdbx::*,
        tables::{self, AccountChange, BitmapKey, StorageChange, StorageChangeKey},
        traits::*,
    },
    models::*,
    stagedsync::stage::*,
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

pub const ACCOUNT_HISTORY_INDEX: StageId = StageId("AccountHistoryIndex");
pub const STORAGE_HISTORY_INDEX: StageId = StageId("StorageHistoryIndex");

/// Generate account history index
#[derive(Debug)]
pub struct AccountHistoryIndex {
    pub temp_dir: Arc<TempDir>,
    pub flush_interval: u64,
}

impl AccountHistoryIndex {
    fn execute<'db, 'tx, E>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
        E: EnvironmentKind,
    {
        Ok(execute_index(
            tx,
            input,
            &self.temp_dir,
            self.flush_interval,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |block_number, AccountChange { address, .. }| (block_number, address),
        )?)
    }

    fn unwind<'db, 'tx, E>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
        E: EnvironmentKind,
    {
        unwind_index(
            tx,
            input,
            tables::AccountChangeSet,
            tables::AccountHistory,
            |_, AccountChange { address, .. }| address,
        )
    }
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
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        Self::execute(self, tx, input)
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        Self::unwind(self, tx, input)
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
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        Ok(execute_index(
            tx,
            input,
            &self.temp_dir,
            self.flush_interval,
            tables::StorageChangeSet,
            tables::StorageHistory,
            |StorageChangeKey {
                 block_number,
                 address,
             },
             StorageChange { location, .. }| (block_number, (address, location)),
        )?)
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
        reached_tip: true,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{kv::new_mem_chaindata, stages};

    fn collect_bitmap_and_check<'db, K: TransactionKind, E: EnvironmentKind>(
        tx: &MdbxTransaction<'db, K, E>,
        changed_blocks: &BTreeSet<u64>,
        address: Address,
        limit: u64,
    ) {
        let walker = tx
            .cursor(tables::AccountHistory)
            .unwrap()
            .walk(Some(BitmapKey {
                inner: address,
                block_number: BlockNumber(0),
            }));

        pin!(walker);

        let mut indexed_changed_blocks = BTreeSet::new();
        while let Some((key, bitmap)) = walker.next().transpose().unwrap() {
            if key.inner != address {
                break;
            }

            for value in bitmap.iter() {
                if let Some(&last_indexed) = indexed_changed_blocks.last() {
                    assert!(last_indexed < value);
                }
                indexed_changed_blocks.insert(value);
            }

            if key.block_number.0 != u64::MAX {
                assert_eq!(*indexed_changed_blocks.last().unwrap(), key.block_number.0);
            }
        }

        assert_eq!(
            changed_blocks
                .iter()
                .copied()
                .take_while(|v| v <= &limit)
                .collect::<BTreeSet<_>>(),
            indexed_changed_blocks
        );
    }

    #[test]
    fn execute_account_index() {
        let chaindata = new_mem_chaindata().unwrap();

        let mut tx = chaindata.begin_mutable().unwrap();

        const LIMIT: u64 = 2_000_000;

        let address = Address::from_low_u64_be(0x42);
        let mut account = Account {
            nonce: 0,
            balance: U256::ZERO,
            code_hash: EMPTY_HASH,
        };
        let mut changed_blocks = BTreeSet::new();
        for block in 0..LIMIT {
            if block % 2 == 0 {
                changed_blocks.insert(block);
                account.nonce = block * 2;
                tx.set(
                    tables::AccountChangeSet,
                    BlockNumber(block),
                    AccountChange {
                        address,
                        account: Some(account),
                    },
                )
                .unwrap();
            }
        }

        let mut stage = AccountHistoryIndex {
            temp_dir: Arc::new(TempDir::new().unwrap()),
            flush_interval: LIMIT,
        };

        let mut stage_progress = None;
        for limit in [LIMIT / 2, LIMIT] {
            stage.flush_interval = limit / 3;
            let executed = stage
                .execute(
                    &mut tx,
                    StageInput {
                        restarted: false,
                        first_started_at: (Instant::now(), None),
                        previous_stage: Some((stages::EXECUTION, BlockNumber(limit))),
                        stage_progress,
                    },
                )
                .unwrap();

            assert_eq!(
                executed,
                ExecOutput::Progress {
                    stage_progress: BlockNumber(limit),
                    done: true,
                    reached_tip: true,
                },
            );
            stage_progress = Some(BlockNumber(limit));

            collect_bitmap_and_check(&tx, &changed_blocks, address, limit);
        }

        assert_eq!(
            stage
                .unwind(
                    &mut tx,
                    UnwindInput {
                        stage_progress: stage_progress.unwrap(),
                        unwind_to: BlockNumber(LIMIT / 2),
                        bad_block: None,
                    },
                )
                .unwrap(),
            UnwindOutput {
                stage_progress: BlockNumber(LIMIT / 2)
            }
        );

        collect_bitmap_and_check(&tx, &changed_blocks, address, LIMIT / 2);
    }
}
