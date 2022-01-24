use super::{
    downloader_stage_loop::DownloaderStageLoop,
    headers::{
        header_slices,
        header_slices::{align_block_num_to_slice_start, HeaderSliceStatus, HeaderSlices},
    },
    headers_ui::HeaderSlicesView,
    stages::*,
    ui::ui_system::{UISystemShared, UISystemViewScope},
    verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{
    kv,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, messages::BlockHashAndNumber, sentry_client_reactor::*},
};
use std::sync::Arc;
use tracing::*;

#[derive(Debug)]
pub struct DownloaderLinear {
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    mem_limit: usize,
    sentry: SentryClientReactorShared,
}

pub struct DownloaderLinearReport {
    pub loaded_count: usize,
    pub final_block_num: BlockNumber,
    pub target_final_block_num: BlockNumber,
    pub estimated_top_block_num: BlockNumber,
}

impl DownloaderLinear {
    pub fn new(
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
        mem_limit: usize,
        sentry: SentryClientReactorShared,
    ) -> Self {
        Self {
            chain_config,
            verifier,
            mem_limit,
            sentry,
        }
    }

    async fn estimate_top_block_num(
        &self,
        start_block_num: BlockNumber,
    ) -> anyhow::Result<BlockNumber> {
        info!("DownloaderLinear: waiting to estimate a top block number...");
        let stage = TopBlockEstimateStage::new(self.sentry.clone());
        while !stage.is_over() && stage.estimated_top_block_num().is_none() {
            stage.execute().await?;
        }
        let estimated_top_block_num = stage.estimated_top_block_num().unwrap_or(start_block_num);
        info!(
            "DownloaderLinear: estimated top block number = {}",
            estimated_top_block_num.0
        );
        Ok(estimated_top_block_num)
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_id: BlockHashAndNumber,
        estimated_top_block_num: Option<BlockNumber>,
        max_blocks_count: usize,
        ui_system: UISystemShared,
    ) -> anyhow::Result<DownloaderLinearReport> {
        let start_block_num = start_block_id.number;

        let trusted_len: u64 = 90_000;

        let estimated_top_block_num = match estimated_top_block_num {
            Some(block_num) => block_num,
            None => self.estimate_top_block_num(start_block_num).await?,
        };

        let target_final_block_num = if estimated_top_block_num.0 > trusted_len {
            align_block_num_to_slice_start(BlockNumber(estimated_top_block_num.0 - trusted_len))
        } else {
            BlockNumber(0)
        };
        let final_block_num = BlockNumber(std::cmp::min(
            target_final_block_num.0,
            align_block_num_to_slice_start(BlockNumber(
                start_block_num.0 + (max_blocks_count as u64),
            ))
            .0,
        ));

        if start_block_num.0 >= final_block_num.0 {
            return Ok(DownloaderLinearReport {
                loaded_count: 0,
                final_block_num: start_block_num,
                target_final_block_num,
                estimated_top_block_num,
            });
        }

        let header_slices = Arc::new(HeaderSlices::new(
            self.mem_limit,
            start_block_num,
            final_block_num,
        ));
        let sentry = self.sentry.clone();

        let header_slices_view = HeaderSlicesView::new(header_slices.clone(), "DownloaderLinear");
        let _header_slices_view_scope =
            UISystemViewScope::new(&ui_system, Box::new(header_slices_view));

        let fetch_request_stage = FetchRequestStage::new(
            header_slices.clone(),
            sentry.clone(),
            header_slices::HEADER_SLICE_SIZE,
        );
        let fetch_receive_stage = FetchReceiveStage::new(header_slices.clone(), sentry.clone());
        let retry_stage = RetryStage::new(header_slices.clone());
        let verify_stage = VerifyStageLinear::new(
            header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
        );
        let verify_link_stage = VerifyStageLinearLink::new(
            header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
            start_block_num,
            start_block_id.hash,
            HeaderSliceStatus::Invalid,
        );
        let penalize_stage = PenalizeStage::new(header_slices.clone(), sentry.clone());
        let save_stage = SaveStage::<RwTx>::new(
            header_slices.clone(),
            db_transaction,
            save_stage::SaveOrder::Monotonic,
            true,
        );
        let refill_stage = RefillStage::new(header_slices.clone());

        let refill_stage_is_over = refill_stage.is_over_check();

        let mut stages = DownloaderStageLoop::new(&header_slices, None);
        stages.insert(fetch_request_stage);
        stages.insert(fetch_receive_stage);
        stages.insert(retry_stage);
        stages.insert(verify_stage);
        stages.insert(verify_link_stage);
        stages.insert(penalize_stage);
        stages.insert(save_stage);
        stages.insert(refill_stage);

        stages.run(refill_stage_is_over).await;

        let report = DownloaderLinearReport {
            loaded_count: (header_slices.min_block_num().0 - start_block_num.0) as usize,
            final_block_num: header_slices.min_block_num(),
            target_final_block_num,
            estimated_top_block_num,
        };

        Ok(report)
    }
}
