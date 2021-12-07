use crate::{
    downloader::{sentry_status_provider::SentryStatusProvider, Downloader},
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::SentryClientReactorShared},
    stagedsync::stage::{ExecOutput, Stage, StageInput},
    MutableTransaction, StageId,
};
use async_trait::async_trait;

#[derive(Debug)]
pub struct HeaderDownload {
    downloader: Downloader,
    batch_size: usize,
}

impl HeaderDownload {
    pub fn new(
        chain_config: ChainConfig,
        batch_size: usize,
        sentry: SentryClientReactorShared,
        sentry_status_provider: SentryStatusProvider,
    ) -> Self {
        let downloader = Downloader::new(chain_config, sentry, sentry_status_provider);

        Self {
            downloader,
            batch_size,
        }
    }
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for HeaderDownload
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("HeaderDownload")
    }

    fn description(&self) -> &'static str {
        "Downloading headers"
    }

    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let past_progress = input.stage_progress.unwrap_or_default();

        let start_block_num = BlockNumber(past_progress.0 + 1);
        let report = self
            .downloader
            .run(tx, start_block_num, self.batch_size)
            .await?;

        let final_block_num = report.final_block_num.0;
        let stage_progress = if final_block_num > 0 {
            BlockNumber(final_block_num - 1)
        } else {
            past_progress
        };

        let done = final_block_num >= report.target_final_block_num.0;

        Ok(ExecOutput::Progress {
            stage_progress,
            done,
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
