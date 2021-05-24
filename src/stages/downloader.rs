use std::time::Duration;

use crate::{
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    MutableTransaction, SyncStage,
};
use async_trait::async_trait;
use rand::Rng;
use tokio::time::sleep;
use tracing::*;

#[derive(Debug)]
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
        info!("Processing headers");

        let target = past_progress + 100;

        let commit_block =
            rand::random::<bool>().then(|| past_progress + rand::thread_rng().gen_range(0..target));

        let mut processed = past_progress;
        let mut must_commit = false;
        for block in past_progress..=target {
            info!(block = block, "(mock) Downloading");

            processed += 1;

            if let Some(commit_block) = commit_block {
                if block == commit_block {
                    must_commit = true;
                    break;
                }
            }

            let dur = Duration::from_millis(rand::thread_rng().gen_range(0..500));
            sleep(dur).await;
        }
        info!(highest = target, "Processed");
        Ok(ExecOutput::Progress {
            stage_progress: processed,
            done: !must_commit,
            must_commit,
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
