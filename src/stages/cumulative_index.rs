use crate::{
    kv::{
        tables::{self, CumulativeData},
        traits::{Cursor, MutableCursor},
    },
    stagedsync::stage::*,
    MutableTransaction, StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use tracing::*;

#[derive(Debug)]
pub struct CumulativeIndex;

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for CumulativeIndex
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("CumulativeIndex")
    }

    fn description(&self) -> &'static str {
        ""
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let prev_progress = input.stage_progress.unwrap_or_default();

        let mut cumulative_index_cur = tx.mutable_cursor(&tables::CumulativeIndex).await?;

        let starting_block = prev_progress + 1;
        let max_block = input
            .previous_stage
            .map(|(_, v)| v)
            .ok_or_else(|| format_err!("Cannot be the first stage"))?;

        if max_block >= starting_block {
            let CumulativeData {
                mut gas,
                mut tx_num,
            } = cumulative_index_cur
                .seek_exact(prev_progress)
                .await?
                .unwrap()
                .1;

            for block_num in starting_block..=max_block {
                if block_num.0 % 500_000 == 0 {
                    info!("Building cumulative index for block {}", block_num);
                }

                let canonical_hash = tx.get(&tables::CanonicalHeader, block_num).await?.unwrap();
                let header = tx
                    .get(&tables::Header, (block_num, canonical_hash))
                    .await?
                    .unwrap();
                let body = tx
                    .get(&tables::BlockBody, (block_num, canonical_hash))
                    .await?
                    .unwrap();

                gas += header.gas_used;
                tx_num += body.tx_amount as u64;

                cumulative_index_cur
                    .append(block_num, CumulativeData { gas, tx_num })
                    .await?;
            }
        }

        Ok(ExecOutput::Progress {
            stage_progress: max_block,
            done: true,
            must_commit: max_block > prev_progress,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut cumulative_index_cur = tx.mutable_cursor(&tables::CumulativeIndex).await?;

        while let Some((block_num, _)) = cumulative_index_cur.last().await? {
            if block_num > input.unwind_to {
                cumulative_index_cur.delete_current().await?;
            } else {
                break;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
            must_commit: true,
        })
    }
}
