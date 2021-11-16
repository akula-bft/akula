use super::{
    fetch_receive_stage::FetchReceiveStage, fetch_request_stage::FetchRequestStage, header_slices,
    header_slices::HeaderSlices, refill_stage::RefillStage, retry_stage::RetryStage,
    save_stage::SaveStage, verify_stage_linear::VerifyStageLinear,
    verify_stage_linear_link::VerifyStageLinearLink, HeaderSlicesView,
};
use crate::{
    downloader::{
        headers::stage_stream::{make_stage_stream, StageStream},
        ui_system::UISystem,
    },
    kv,
    models::BlockNumber,
    sentry::sentry_client_reactor::SentryClientReactor,
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;

pub struct DownloaderLinear<DB: kv::traits::MutableKV + Sync> {
    chain_name: String,
    start_block_num: BlockNumber,
    start_block_hash: ethereum_types::H256,
    mem_limit: usize,
    sentry: Arc<RwLock<SentryClientReactor>>,
    db: Arc<DB>,
    ui_system: Arc<Mutex<UISystem>>,
}

impl<DB: kv::traits::MutableKV + Sync> DownloaderLinear<DB> {
    pub fn new(
        chain_name: String,
        start_block_num: BlockNumber,
        start_block_hash: ethereum_types::H256,
        mem_limit: usize,
        sentry: Arc<RwLock<SentryClientReactor>>,
        db: Arc<DB>,
        ui_system: Arc<Mutex<UISystem>>,
    ) -> Self {
        Self {
            chain_name,
            start_block_num,
            start_block_hash,
            mem_limit,
            sentry,
            db,
            ui_system,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let header_slices_mem_limit = self.mem_limit;

        let trusted_len: u64 = 90_000;
        let estimated_latest_block_num: u64 = 13_000_000;
        let slice_size = header_slices::HEADER_SLICE_SIZE as u64;
        let header_slices_final_block_num =
            BlockNumber((estimated_latest_block_num - trusted_len) / slice_size * slice_size);

        let header_slices = Arc::new(HeaderSlices::new(
            header_slices_mem_limit,
            self.start_block_num,
            header_slices_final_block_num,
        ));
        let sentry = self.sentry.clone();

        let header_slices_view = HeaderSlicesView::new(header_slices.clone());
        self.ui_system
            .try_lock()?
            .set_view(Some(Box::new(header_slices_view)));

        // Downloading happens with several stages where
        // each of the stages processes blocks in one status,
        // and updates them to proceed to the next status.
        // All stages runs in parallel,
        // although most of the time only one of the stages is actively running,
        // while the others are waiting for the status updates or timeouts.

        let fetch_request_stage = FetchRequestStage::new(
            header_slices.clone(),
            sentry.clone(),
            header_slices::HEADER_SLICE_SIZE,
        );
        let fetch_receive_stage = FetchReceiveStage::new(header_slices.clone(), sentry.clone());
        let retry_stage = RetryStage::new(header_slices.clone());
        let verify_stage = VerifyStageLinear::new(header_slices.clone());
        let verify_link_stage = VerifyStageLinearLink::new(
            header_slices.clone(),
            self.start_block_num,
            self.start_block_hash,
        );
        let save_stage = SaveStage::new(header_slices.clone(), self.db.clone());
        let refill_stage = RefillStage::new(header_slices.clone());

        let can_proceed = fetch_receive_stage.can_proceed_checker();

        let mut stream = StreamMap::<&str, StageStream>::new();
        stream.insert(
            "fetch_request_stage",
            make_stage_stream(Box::new(fetch_request_stage)),
        );
        stream.insert(
            "fetch_receive_stage",
            make_stage_stream(Box::new(fetch_receive_stage)),
        );
        stream.insert("retry_stage", make_stage_stream(Box::new(retry_stage)));
        stream.insert("verify_stage", make_stage_stream(Box::new(verify_stage)));
        stream.insert(
            "verify_link_stage",
            make_stage_stream(Box::new(verify_link_stage)),
        );
        stream.insert("save_stage", make_stage_stream(Box::new(save_stage)));
        stream.insert("refill_stage", make_stage_stream(Box::new(refill_stage)));

        while let Some((key, result)) = stream.next().await {
            if result.is_err() {
                error!("Downloader headers {} failure: {:?}", key, result);
                break;
            }

            if !can_proceed.can_proceed() {
                break;
            }
            if header_slices.is_empty_at_final_position() {
                break;
            }

            header_slices.notify_status_watchers();
        }

        Ok(())
    }
}
