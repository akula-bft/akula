use super::{
    downloader_forky, downloader_linear, downloader_preverified,
    headers::header_slices::HeaderSlices, stages::fork_switch_command::ForkSwitchCommand,
    ui::ui_system::UISystemShared, verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{
    kv,
    models::*,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::*},
};
use parking_lot::Mutex;
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

#[derive(Debug)]
pub struct Downloader {
    downloader_preverified: downloader_preverified::DownloaderPreverified,
    downloader_linear: downloader_linear::DownloaderLinear,
    downloader_forky: downloader_forky::DownloaderForky,
    genesis_block_hash: H256,
}

pub struct DownloaderReport {
    pub final_block_num: BlockNumber,
    pub target_final_block_num: BlockNumber,
    pub run_state: DownloaderRunState,
}

#[derive(Clone)]
pub struct DownloaderRunState {
    pub estimated_top_block_num: Option<BlockNumber>,
    pub forky_header_slices: Option<Arc<HeaderSlices>>,
    pub forky_fork_header_slices: Option<Arc<HeaderSlices>>,
    pub unwind_request: Option<DownloaderUnwindRequest>,
}

#[derive(Clone)]
pub struct DownloaderUnwindRequest {
    pub unwind_to_block_num: BlockNumber,
    finalize: Arc<Mutex<Option<ForkSwitchCommand>>>,
}

impl Debug for DownloaderRunState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloaderRunState")
            .field("estimated_top_block_num", &self.estimated_top_block_num)
            .field("forky_header_slices", &self.forky_header_slices.is_some())
            .field(
                "forky_fork_header_slices",
                &self.forky_fork_header_slices.is_some(),
            )
            .finish()
    }
}

impl From<ForkSwitchCommand> for DownloaderUnwindRequest {
    fn from(command: ForkSwitchCommand) -> Self {
        Self {
            unwind_to_block_num: command.connection_block_num(),
            finalize: Arc::new(Mutex::new(Some(command))),
        }
    }
}

impl Downloader {
    pub fn new(
        chain_config: ChainConfig,
        verifier: Box<dyn HeaderSliceVerifier>,
        mem_limit: usize,
        sentry: SentryClientReactorShared,
    ) -> anyhow::Result<Self> {
        let verifier = Arc::new(verifier);

        let downloader_preverified = downloader_preverified::DownloaderPreverified::new(
            verifier.preverified_hashes_config(&chain_config.chain_name())?,
            mem_limit,
            sentry.clone(),
        );

        let downloader_linear = downloader_linear::DownloaderLinear::new(
            chain_config.clone(),
            verifier.clone(),
            mem_limit,
            sentry.clone(),
        );

        let downloader_forky =
            downloader_forky::DownloaderForky::new(chain_config.clone(), verifier, sentry);

        let instance = Self {
            downloader_preverified,
            downloader_linear,
            downloader_forky,
            genesis_block_hash: chain_config.genesis_block_hash(),
        };
        Ok(instance)
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_num: BlockNumber,
        max_blocks_count: usize,
        previous_run_state: Option<DownloaderRunState>,
        ui_system: UISystemShared,
    ) -> anyhow::Result<DownloaderReport> {
        let mut max_blocks_count = max_blocks_count;

        let preverified_report = self
            .downloader_preverified
            .run::<RwTx>(
                db_transaction,
                start_block_num,
                max_blocks_count,
                ui_system.clone(),
            )
            .await?;
        max_blocks_count -= preverified_report.loaded_count;

        let linear_estimated_top_block_num =
            preverified_report.estimated_top_block_num.or_else(|| {
                previous_run_state
                    .as_ref()
                    .and_then(|state| state.estimated_top_block_num)
            });

        let linear_report = self
            .downloader_linear
            .run::<RwTx>(
                db_transaction,
                preverified_report.final_block_num,
                max_blocks_count,
                linear_estimated_top_block_num,
                ui_system.clone(),
            )
            .await?;
        max_blocks_count -= linear_report.loaded_count;

        let forky_report = self
            .downloader_forky
            .run::<RwTx>(
                db_transaction,
                linear_report.final_block_num,
                max_blocks_count,
                linear_report.target_final_block_num,
                previous_run_state
                    .as_ref()
                    .and_then(|state| state.forky_header_slices.clone()),
                previous_run_state
                    .as_ref()
                    .and_then(|state| state.forky_fork_header_slices.clone()),
                ui_system,
            )
            .await?;
        // max_blocks_count -= forky_report.loaded_count;

        let report = DownloaderReport {
            final_block_num: forky_report.final_block_num,
            target_final_block_num: linear_report.target_final_block_num,
            run_state: DownloaderRunState {
                estimated_top_block_num: Some(linear_report.estimated_top_block_num),
                forky_header_slices: forky_report.header_slices,
                forky_fork_header_slices: forky_report.fork_header_slices,
                unwind_request: forky_report
                    .termination_command
                    .map(DownloaderUnwindRequest::from),
            },
        };

        Ok(report)
    }

    pub async fn unwind<
        'downloader,
        'db: 'downloader,
        RwTx: kv::traits::MutableTransaction<'db>,
    >(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        unwind_to_block_num: BlockNumber,
    ) -> anyhow::Result<()> {
        super::stages::SaveStage::unwind(unwind_to_block_num, db_transaction).await
    }

    pub async fn unwind_finalize<
        'downloader,
        'db: 'downloader,
        RwTx: kv::traits::MutableTransaction<'db>,
    >(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        unwind_request: DownloaderUnwindRequest,
    ) -> anyhow::Result<()> {
        let Some(finalize) = unwind_request.finalize.lock().take() else {
            anyhow::bail!("unwind_finalize: finalize command expected in unwind_request");
        };
        finalize.execute(db_transaction).await
    }
}
