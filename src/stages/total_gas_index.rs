use crate::{
    accessors,
    kv::{mdbx::*, tables},
    stagedsync::stage::*,
    StageId,
};
use anyhow::format_err;
use async_trait::async_trait;
use tracing::*;

pub const TOTAL_GAS_INDEX: StageId = StageId("TotalGasIndex");

#[derive(Debug)]
pub struct TotalGasIndex;

#[async_trait]
impl<'db, E> Stage<'db, E> for TotalGasIndex
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        TOTAL_GAS_INDEX
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

        let mut cumulative_index_cur = tx.cursor(tables::TotalGas)?;

        let starting_block = prev_progress + 1;
        let max_block = input
            .previous_stage
            .map(|(_, v)| v)
            .ok_or_else(|| format_err!("Cannot be the first stage"))?;

        if max_block >= starting_block {
            let mut gas = cumulative_index_cur.seek_exact(prev_progress)?.unwrap().1;

            for block_num in starting_block..=max_block {
                if block_num.0 % 500_000 == 0 {
                    info!("Building total gas index for block {block_num}");
                }

                let header = accessors::chain::header::read(tx, block_num)?
                    .ok_or_else(|| format_err!("No header for block #{block_num}"))?;

                gas += header.gas_used;

                cumulative_index_cur.append(block_num, gas)?;
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
        let mut cumulative_index_cur = tx.cursor(tables::TotalGas)?;

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
