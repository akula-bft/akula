use super::{
    downloader_stage_loop::DownloaderStageLoop,
    headers::{
        header_slices,
        header_slices::{align_block_num_to_slice_start, HeaderSlices},
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

#[derive(Debug)]
pub struct DownloaderForky {
    chain_config: ChainConfig,
    verifier: Arc<Box<dyn HeaderSliceVerifier>>,
    sentry: SentryClientReactorShared,
}

pub struct DownloaderForkyReport {
    pub loaded_count: usize,
    pub final_block_num: BlockNumber,
    pub header_slices: Option<Arc<HeaderSlices>>,
}

impl DownloaderForky {
    pub fn new(
        chain_config: ChainConfig,
        verifier: Arc<Box<dyn HeaderSliceVerifier>>,
        sentry: SentryClientReactorShared,
    ) -> Self {
        Self {
            chain_config,
            verifier,
            sentry,
        }
    }

    fn make_header_slices(
        start_block_num: BlockNumber,
        forky_max_blocks_count: usize,
    ) -> HeaderSlices {
        // This is more than enough to store forky_max_blocks_count blocks.
        // It's not gonna affect the window size or memory usage.
        let mem_limit = byte_unit::n_gib_bytes!(1) as usize;

        let final_block_num = align_block_num_to_slice_start(BlockNumber(
            start_block_num.0 + (forky_max_blocks_count as u64),
        ));

        HeaderSlices::new(mem_limit, start_block_num, final_block_num)
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_id: BlockHashAndNumber,
        max_blocks_count: usize,
        previous_run_header_slices: Option<Arc<HeaderSlices>>,
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
                header_slices: previous_run_header_slices,
            });
        }

        let header_slices = previous_run_header_slices.unwrap_or_else(|| {
            Arc::new(Self::make_header_slices(
                start_block_num,
                forky_max_blocks_count,
            ))
        });
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
            self.chain_config.clone(),
            self.verifier.clone(),
        );
        let verify_link_stage = VerifyStageForkyLink::new(
            header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
            start_block_num,
            start_block_id.hash,
        );
        let refetch_stage = RefetchStage::new(header_slices.clone());
        let penalize_stage = PenalizeStage::new(header_slices.clone(), sentry.clone());
        let save_stage = SaveStage::<RwTx>::new(header_slices.clone(), db_transaction);

        let is_over_check = || -> bool { false };

        let mut stages = DownloaderStageLoop::new(&header_slices);
        stages.insert(fetch_request_stage);
        stages.insert(fetch_receive_stage);
        stages.insert(retry_stage);
        stages.insert(verify_stage);
        stages.insert(verify_link_stage);
        stages.insert(refetch_stage);
        stages.insert(penalize_stage);
        stages.insert(save_stage);

        stages.run(is_over_check).await;

        let report = DownloaderForkyReport {
            loaded_count: (header_slices.min_block_num().0 - start_block_num.0) as usize,
            final_block_num: header_slices.min_block_num(),
            header_slices: Some(header_slices),
        };

        Ok(report)
    }
}
