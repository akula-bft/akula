use super::{
    downloader_stage_loop::DownloaderStageLoop, fetch_receive_stage::FetchReceiveStage,
    fetch_request_stage::FetchRequestStage, header_slices, header_slices::HeaderSlices,
    penalize_stage::PenalizeStage, retry_stage::RetryStage, save_stage::SaveStage,
    verify_stage_forky_link::VerifyStageForkyLink, verify_stage_linear::VerifyStageLinear,
    HeaderSlicesView,
};
use crate::{
    downloader::{
        headers::header_slices::align_block_num_to_slice_start,
        ui_system::{UISystemShared, UISystemViewScope},
    },
    kv,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, messages::BlockHashAndNumber, sentry_client_reactor::*},
};
use std::sync::Arc;

#[derive(Debug)]
pub struct DownloaderForky {
    chain_config: ChainConfig,
    sentry: SentryClientReactorShared,
}

pub struct DownloaderForkyReport {
    pub loaded_count: usize,
    pub final_block_num: BlockNumber,
}

impl DownloaderForky {
    pub fn new(chain_config: ChainConfig, sentry: SentryClientReactorShared) -> Self {
        Self {
            chain_config,
            sentry,
        }
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_id: BlockHashAndNumber,
        max_blocks_count: usize,
        ui_system: UISystemShared,
    ) -> anyhow::Result<DownloaderForkyReport> {
        let start_block_num = start_block_id.number;

        // Assuming we've downloaded all but last 90K headers in previous phases
        // we need to download them now, plus a bit more,
        // because extra blocks have been generating while downloading.
        // (ropsten/mainnet generate about 6500K blocks per day, and the sync is hopefully faster)
        // It must be less than Opts::headers_batch_size to pass the max_blocks_count check below.
        let forky_max_blocks_count: usize = 99_000;

        if max_blocks_count < forky_max_blocks_count {
            return Ok(DownloaderForkyReport {
                loaded_count: 0,
                final_block_num: start_block_num,
            });
        }

        // This is more than enough to store forky_max_blocks_count blocks.
        // It's not gonna affect the window size or memory usage.
        let mem_limit = byte_unit::n_gib_bytes!(1) as usize;

        let final_block_num = align_block_num_to_slice_start(BlockNumber(
            start_block_num.0 + (forky_max_blocks_count as u64),
        ));

        let header_slices = Arc::new(HeaderSlices::new(
            mem_limit,
            start_block_num,
            final_block_num,
        ));
        let sentry = self.sentry.clone();

        let header_slices_view = HeaderSlicesView::new(header_slices.clone(), "DownloaderForky");
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
            header_slices::HEADER_SLICE_SIZE,
            self.chain_config.clone(),
        );
        let verify_link_stage = VerifyStageForkyLink::new(
            header_slices.clone(),
            self.chain_config.clone(),
            start_block_num,
            start_block_id.hash,
        );
        let penalize_stage = PenalizeStage::new(header_slices.clone(), sentry.clone());
        let save_stage = SaveStage::<RwTx>::new(header_slices.clone(), db_transaction);

        let can_proceed = fetch_receive_stage.can_proceed_check();

        let mut stages = DownloaderStageLoop::new(&header_slices);
        stages.insert(fetch_request_stage);
        stages.insert(fetch_receive_stage);
        stages.insert(retry_stage);
        stages.insert(verify_stage);
        stages.insert(verify_link_stage);
        stages.insert(penalize_stage);
        stages.insert(save_stage);

        stages.run(can_proceed).await;

        let report = DownloaderForkyReport {
            loaded_count: (header_slices.min_block_num().0 - start_block_num.0) as usize,
            final_block_num: header_slices.min_block_num(),
        };

        Ok(report)
    }
}
