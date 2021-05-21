use std::time::Duration;

use crate::{
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    MutableTransaction, SyncStage,
};
use async_trait::async_trait;
use rand::Rng;
use tokio::time::sleep;

pub struct HeaderDownload;

#[async_trait(?Send)]
impl<'db, RwTx> Stage<'db, RwTx> for HeaderDownload
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> SyncStage {
        SyncStage("HeaderDownload")
    }

    fn description(&self) -> &'static str {
        "Downloading headers"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let _ = tx;
        let past_progress = input.stage_progress.unwrap_or(0);
        let target = past_progress + 100;
        for block in past_progress..target {
            input
                .logger
                .info(format!("(mock) Downloading header {}", block));

            let dur = Duration::from_millis(rand::thread_rng().gen_range(50..500));
            sleep(dur).await;
        }
        Ok(ExecOutput::Progress {
            stage_progress: target,
            done: true,
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
