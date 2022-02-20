use crate::{
    downloader::{
        sentry_status_provider::SentryStatusProvider, ui::ui_system::UISystem, HeadersDownloader,
        HeadersDownloaderRunState,
    },
    kv::mdbx::*,
    models::*,
    sentry_connector::{
        chain_config::ChainConfig, sentry_client_reactor::SentryClientReactorShared,
    },
    stagedsync::{stage::*, stages::HEADERS},
    StageId,
};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// Download of headers
#[derive(Debug)]
pub struct HeaderDownload {
    downloader: HeadersDownloader,
    batch_size: usize,
    sentry_status_provider: SentryStatusProvider,
    previous_run_state: Arc<Mutex<Option<HeadersDownloaderRunState>>>,
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
            previous_run_state: Arc::new(Mutex::new(None)),
        };
        Ok(instance)
    }

    fn load_previous_run_state(&self) -> Option<HeadersDownloaderRunState> {
        self.previous_run_state.lock().clone()
    }

    fn save_run_state(&self, run_state: HeadersDownloaderRunState) {
        *self.previous_run_state.lock() = Some(run_state);
    }
}

#[async_trait]
impl<'db, E> Stage<'db, E> for HeaderDownload
where
    E: EnvironmentKind,
{
    fn id(&self) -> StageId {
        HEADERS
    }

    async fn execute<'tx>(
        &mut self,
        tx: &'tx mut MdbxTransaction<'db, RW, E>,
        input: StageInput,
    ) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        self.sentry_status_provider.update(tx)?;

        // finalize unwind request
        if let Some(mut state) = self.load_previous_run_state() {
            if let Some(unwind_request) = state.unwind_request.take() {
                self.downloader.unwind_finalize(tx, unwind_request)?;
                self.save_run_state(state);
            }
        }

        let past_progress = input.stage_progress.unwrap_or_default();
        let start_block_num = BlockNumber(past_progress.0 + 1);

        let previous_run_state = self.load_previous_run_state();

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

        if let Some(unwind_request) = &report.run_state.unwind_request {
            let unwind_to = unwind_request.unwind_to_block_num;
            self.save_run_state(report.run_state);
            return Ok(ExecOutput::Unwind { unwind_to });
        }

        self.save_run_state(report.run_state);

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
        self.downloader.unwind(tx, input.unwind_to)?;

        let stage_progress = BlockNumber(std::cmp::min(input.stage_progress.0, input.unwind_to.0));
        Ok(UnwindOutput { stage_progress })
    }
}
