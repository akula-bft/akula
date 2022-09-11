use crate::{kv::mdbx::*, models::*, stagedsync::stage::*, StageId};
use async_trait::async_trait;

pub const FINISH: StageId = StageId("Finish");

#[derive(Debug)]
pub struct Finish;

#[async_trait]
impl<'db, E> Stage<'db, E> for Finish
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        FINISH
    }
    async fn execute<'tx>(
        &mut self,
        _: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> Result<ExecOutput, StageError>
    where
        'db: 'tx,
    {
        let prev_stage = input
            .previous_stage
            .map(|(_, b)| b)
            .unwrap_or(BlockNumber(0));

        Ok(ExecOutput::Progress {
            stage_progress: prev_stage,
            done: true,
            reached_tip: true,
        })
    }
    async fn unwind<'tx>(
        &mut self,
        _: &'tx mut MdbxTransaction<'db, RW, E>,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}
