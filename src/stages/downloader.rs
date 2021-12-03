use crate::{
    downloader::{
        sentry_status_provider::SentryStatusProvider, Downloader, HeaderDownloaderRunState,
    },
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::SentryClientReactorShared},
    stagedsync::stage::*,
    MutableTransaction, StageId,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

#[derive(Debug)]
pub struct HeaderDownload {
    downloader: Downloader,
    batch_size: usize,
    previous_run_state: Arc<AsyncMutex<Option<HeaderDownloaderRunState>>>,
}

impl HeaderDownload {
    pub fn new(
        chain_config: ChainConfig,
        mem_limit: usize,
        batch_size: usize,
        sentry: SentryClientReactorShared,
        sentry_status_provider: SentryStatusProvider,
    ) -> anyhow::Result<Self> {
        let downloader = Downloader::new(chain_config, mem_limit, sentry, sentry_status_provider)?;

        let instance = Self {
            downloader,
            batch_size,
            previous_run_state: Arc::new(AsyncMutex::new(None)),
        };
        Ok(instance)
    }

    async fn load_previous_run_state(&self) -> Option<HeaderDownloaderRunState> {
        self.previous_run_state.lock().await.clone()
    }

    async fn save_run_state(&self, run_state: HeaderDownloaderRunState) {
        *self.previous_run_state.lock().await = Some(run_state);
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
        let previous_run_state = self.load_previous_run_state().await;

        let report = self
            .downloader
            .run(tx, start_block_num, self.batch_size, previous_run_state)
            .await?;

        let final_block_num = report.final_block_num.0;
        let stage_progress = if final_block_num > 0 {
            BlockNumber(final_block_num - 1)
        } else {
            past_progress
        };

        let done = final_block_num >= report.target_final_block_num.0;

        self.save_run_state(report.run_state).await;

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
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let _ = tx;
        let _ = input;
        todo!()
    }
}
