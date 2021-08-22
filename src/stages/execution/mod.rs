use crate::{
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    MutableTransaction,
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct Execution;

#[async_trait]
impl<'db, RwTx: MutableTransaction<'db>> Stage<'db, RwTx> for Execution {
    fn id(&self) -> crate::StageId {
        todo!()
    }

    fn description(&self) -> &'static str {
        todo!()
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let _ = tx;
        let stage_progress = input.stage_progress.unwrap_or(0);

        for block_number in stage_progress
            ..input
                .previous_stage
                .map(|(_, b)| b)
                .unwrap_or(stage_progress)
        {
            let _ = block_number;
        }

        Ok(ExecOutput::Progress {
            stage_progress,
            done: true,
            must_commit: true,
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: crate::stagedsync::stage::UnwindInput,
    ) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}
