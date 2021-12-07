use super::{
    fetch_receive_stage::FetchReceiveStage, fetch_request_stage::FetchRequestStage, header_slices,
    header_slices::HeaderSlices, penalize_stage::PenalizeStage,
    preverified_hashes_config::PreverifiedHashesConfig, refill_stage::RefillStage,
    retry_stage::RetryStage, save_stage::SaveStage,
    top_block_estimate_stage::TopBlockEstimateStage,
    verify_stage_preverified::VerifyStagePreverified, HeaderSlicesView,
};
use crate::{
    downloader::{
        headers::{
            header_slices::align_block_num_to_slice_start,
            stage_stream::{make_stage_stream, StageStream},
        },
        ui_system::{UISystem, UISystemViewScope},
    },
    kv,
    models::BlockNumber,
    sentry::sentry_client_reactor::*,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

pub struct DownloaderPreverified {
    preverified_hashes_config: PreverifiedHashesConfig,
    mem_limit: usize,
    sentry: SentryClientReactorShared,
    ui_system: Arc<Mutex<UISystem>>,
}

pub struct DownloaderPreverifiedReport {
    pub loaded_count: usize,
    pub final_block_num: BlockNumber,
    pub target_final_block_num: BlockNumber,
    pub estimated_top_block_num: Option<BlockNumber>,
}

impl DownloaderPreverified {
    pub fn new(
        chain_name: String,
        mem_limit: usize,
        sentry: SentryClientReactorShared,
        ui_system: Arc<Mutex<UISystem>>,
    ) -> anyhow::Result<Self> {
        let preverified_hashes_config = PreverifiedHashesConfig::new(&chain_name)?;

        let instance = Self {
            preverified_hashes_config,
            mem_limit,
            sentry,
            ui_system,
        };
        Ok(instance)
    }

    fn target_final_block_num(&self) -> BlockNumber {
        let slice_size = header_slices::HEADER_SLICE_SIZE as u64;
        BlockNumber((self.preverified_hashes_config.hashes.len() as u64 - 1) * slice_size)
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_num: BlockNumber,
        max_blocks_count: usize,
    ) -> anyhow::Result<DownloaderPreverifiedReport> {
        let start_block_num = align_block_num_to_slice_start(start_block_num);
        let target_final_block_num = self.target_final_block_num();
        let final_block_num = BlockNumber(std::cmp::min(
            target_final_block_num.0,
            align_block_num_to_slice_start(BlockNumber(
                start_block_num.0 + (max_blocks_count as u64),
            ))
            .0,
        ));

        if start_block_num.0 >= final_block_num.0 {
            return Ok(DownloaderPreverifiedReport {
                loaded_count: 0,
                final_block_num: start_block_num,
                target_final_block_num,
                estimated_top_block_num: None,
            });
        }

        let header_slices = Arc::new(HeaderSlices::new(
            self.mem_limit,
            start_block_num,
            final_block_num,
        ));
        let sentry = self.sentry.clone();

        let header_slices_view =
            HeaderSlicesView::new(header_slices.clone(), "DownloaderPreverified");
        let _header_slices_view_scope =
            UISystemViewScope::new(&self.ui_system, Box::new(header_slices_view));

        // Downloading happens with several stages where
        // each of the stages processes blocks in one status,
        // and updates them to proceed to the next status.
        // All stages runs in parallel,
        // although most of the time only one of the stages is actively running,
        // while the others are waiting for the status updates or timeouts.

        let fetch_request_stage = FetchRequestStage::new(
            header_slices.clone(),
            sentry.clone(),
            header_slices::HEADER_SLICE_SIZE + 1,
        );
        let fetch_receive_stage = FetchReceiveStage::new(header_slices.clone(), sentry.clone());
        let retry_stage = RetryStage::new(header_slices.clone());
        let verify_stage = VerifyStagePreverified::new(
            header_slices.clone(),
            self.preverified_hashes_config.clone(),
        );
        let penalize_stage = PenalizeStage::new(header_slices.clone(), sentry.clone());
        let save_stage = SaveStage::<RwTx>::new(header_slices.clone(), db_transaction);
        let refill_stage = RefillStage::new(header_slices.clone());
        let top_block_estimate_stage = TopBlockEstimateStage::new(sentry.clone());

        let can_proceed = fetch_receive_stage.can_proceed_check();
        let estimated_top_block_num_provider =
            top_block_estimate_stage.estimated_top_block_num_provider();

        let mut stream = StreamMap::<&str, StageStream>::new();
        stream.insert(
            "fetch_request_stage",
            make_stage_stream(fetch_request_stage),
        );
        stream.insert(
            "fetch_receive_stage",
            make_stage_stream(fetch_receive_stage),
        );
        stream.insert("retry_stage", make_stage_stream(retry_stage));
        stream.insert("verify_stage", make_stage_stream(verify_stage));
        stream.insert("penalize_stage", make_stage_stream(penalize_stage));
        stream.insert("save_stage", make_stage_stream(save_stage));
        stream.insert("refill_stage", make_stage_stream(refill_stage));
        stream.insert(
            "top_block_estimate_stage",
            make_stage_stream(top_block_estimate_stage),
        );

        while let Some((key, result)) = stream.next().await {
            if result.is_err() {
                error!("Downloader headers {} failure: {:?}", key, result);
                break;
            }

            if !can_proceed() {
                break;
            }
            if header_slices.is_empty_at_final_position() {
                break;
            }

            header_slices.notify_status_watchers();
        }

        let report = DownloaderPreverifiedReport {
            loaded_count: (header_slices.min_block_num().0 - start_block_num.0) as usize,
            final_block_num: header_slices.min_block_num(),
            target_final_block_num,
            estimated_top_block_num: estimated_top_block_num_provider(),
        };

        Ok(report)
    }
}
