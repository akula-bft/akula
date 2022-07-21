use crate::{
    kv::{
        mdbx::*,
        tables::{self, AccountChange, StorageChange, StorageChangeKey},
    },
    stagedsync::stage::*,
    stages::stage_util::*,
    StageId,
};
use async_trait::async_trait;

pub const ACCOUNT_HISTORY_INDEX: StageId = StageId("AccountHistoryIndex");
pub const STORAGE_HISTORY_INDEX: StageId = StageId("StorageHistoryIndex");

/// Generate account history index
#[derive(Debug)]
pub struct AccountHistoryIndex(pub IndexParams);

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
            &self.0,
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
pub struct StorageHistoryIndex(pub IndexParams);

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
            &self.0,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::{new_mem_chaindata, tables::BitmapKey},
        models::{Account, BlockNumber, EMPTY_HASH},
        stages,
    };
    use ethereum_types::Address;
    use ethnum::U256;
    use std::{collections::BTreeSet, sync::Arc, time::Instant};
    use tempfile::TempDir;
    use tokio::pin;

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

        let mut stage = AccountHistoryIndex(IndexParams {
            temp_dir: Arc::new(TempDir::new().unwrap()),
            flush_interval: LIMIT,
        });

        let mut stage_progress = None;
        for limit in [LIMIT / 2, LIMIT] {
            stage.0.flush_interval = limit / 3;
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
