use crate::{
    kv::{mdbx::*, tables},
    stagedsync::stage::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use tracing::*;

pub const TOTAL_TX_INDEX: StageId = StageId("TotalTxIndex");

#[derive(Debug)]
pub struct TotalTxIndex;

#[async_trait]
impl<'db, E> Stage<'db, E> for TotalTxIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        TOTAL_TX_INDEX
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let prev_progress = input.stage_progress.unwrap_or_default();

        let mut cumulative_index_cur = tx.cursor(tables::TotalTx)?;

        let starting_block = prev_progress + 1;
        let max_block = input
            .previous_stage
            .map(|(_, v)| v)
            .ok_or_else(|| format_err!("Cannot be the first stage"))?;

        if max_block >= starting_block {
            let mut tx_num = cumulative_index_cur
                .seek_exact(prev_progress)?
                .ok_or_else(|| {
                    format_err!("Cumulative index not found for block #{prev_progress}")
                })?
                .1;

            for block_num in starting_block..=max_block {
                if block_num.0 % 500_000 == 0 {
                    info!("Building total tx index for block {block_num}");
                }

                let body = tx
                    .get(tables::BlockBody, block_num)?
                    .ok_or_else(|| format_err!("Body not found for block #{block_num}"))?;

                tx_num += body.tx_amount as u64;

                cumulative_index_cur.append(block_num, tx_num)?;
            }
        }

        Ok(ExecOutput::Progress {
            stage_progress: max_block,
            done: true,
            reached_tip: true,
        })
    }

    async fn unwind<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut cumulative_index_cur = tx.cursor(tables::TotalTx)?;

        while let Some((block_num, _)) = cumulative_index_cur.last()? {
            if block_num > input.unwind_to {
                cumulative_index_cur.delete_current()?;
            } else {
                break;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}
