use crate::{
    downloader::{
        sentry_status_provider::SentryStatusProvider, ui::ui_system::UISystem, HeadersDownloader,
        HeadersDownloaderRunState,
    },
    kv::traits::*,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::SentryClientReactorShared},
    stagedsync::{stage::*, stages::HEADERS},
    StageId,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Download of headers
#[derive(Debug)]
pub struct HeaderDownload {
    downloader: HeadersDownloader,
    batch_size: usize,
    sentry_status_provider: SentryStatusProvider,
    previous_run_state: Arc<AsyncMutex<Option<HeadersDownloaderRunState>>>,
}

impl HeaderDownload {
    pub fn new(
        chain_config: ChainConfig,
        mem_limit: usize,
        batch_size: usize,
        sentry: SentryClientReactorShared,
        sentry_status_provider: SentryStatusProvider,
    ) -> anyhow::Result<Self> {
        let verifier = crate::downloader::header_slice_verifier::make_ethash_verifier();

        let downloader = HeadersDownloader::new(chain_config, verifier, mem_limit, sentry)?;

        let instance = Self {
            downloader,
            batch_size,
            sentry_status_provider,
            previous_run_state: Arc::new(AsyncMutex::new(None)),
        };
        Ok(instance)
    }

    async fn load_previous_run_state(&self) -> Option<HeadersDownloaderRunState> {
        self.previous_run_state.lock().await.clone()
    }

    async fn save_run_state(&self, run_state: HeadersDownloaderRunState) {
        *self.previous_run_state.lock().await = Some(run_state);
    }
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for HeaderDownload
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        HEADERS
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut RwTx,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        self.sentry_status_provider.update(tx).await?;

        let past_progress = input.stage_progress.unwrap_or_default();

        let start_block_num = BlockNumber(past_progress.0 + 1);
        let previous_run_state = self.load_previous_run_state().await;

        let mut ui_system = UISystem::new();
        ui_system.start()?;
        let ui_system = Arc::new(AsyncMutex::new(ui_system));

        let report = self
            .downloader
            .run(
                tx,
                start_block_num,
                self.batch_size,
                previous_run_state,
                ui_system.clone(),
            )
            .await?;

        ui_system.try_lock()?.stop().await?;

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
        })
    }

    async fn unwind<'tx>(
        &mut self,
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
