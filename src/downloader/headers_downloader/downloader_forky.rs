use super::{
    downloader_stage_loop::DownloaderStageLoop,
    headers::{
        header::BlockHeader,
        header_slices,
        header_slices::{
            is_block_num_aligned_to_slice_start, HeaderSlice, HeaderSliceStatus, HeaderSlices,
        },
    },
    headers_ui::HeaderSlicesView,
    stages::*,
    ui::ui_system::{UISystemShared, UISystemViewScope},
    verification::header_slice_verifier::HeaderSliceVerifier,
};
use crate::{
    kv,
    kv::tables::HeaderKey,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::*},
};
use std::{ops::ControlFlow, sync::Arc, time::Duration};

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
    pub fork_header_slices: Option<Arc<HeaderSlices>>,
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

    async fn load_header_slices<'tx, 'db: 'tx>(
        db_transaction: &'tx impl kv::traits::MutableTransaction<'db>,
        start_block_num: BlockNumber,
        max_blocks_count: usize,
    ) -> anyhow::Result<HeaderSlices> {
        let max_slices = max_blocks_count / header_slices::HEADER_SLICE_SIZE;
        let max_blocks_count = max_slices * header_slices::HEADER_SLICE_SIZE;

        let mut header_keys = Vec::<HeaderKey>::with_capacity(max_blocks_count);
        for i in 0..max_blocks_count {
            let num = BlockNumber(start_block_num.0 + i as u64);
            if let Some(hash) = db_transaction.get(kv::tables::CanonicalHeader, num).await? {
                header_keys.push((num, hash));
            } else {
                break;
            }
        }

        let mut slices =
            Vec::<HeaderSlice>::with_capacity(header_keys.len() / header_slices::HEADER_SLICE_SIZE);
        for slice_header_keys in header_keys.chunks_exact(header_slices::HEADER_SLICE_SIZE) {
            let mut headers = Vec::<BlockHeader>::with_capacity(header_slices::HEADER_SLICE_SIZE);
            let mut is_full_slice = true;

            for header_key in slice_header_keys {
                if let Some(header) = db_transaction.get(kv::tables::Header, *header_key).await? {
                    let known_hash = header_key.1;
                    let header = BlockHeader::new(header, known_hash);
                    headers.push(header);
                } else {
                    tracing::warn!("load_header_slices: header {:?} not found although present in tables::CanonicalHeader", header_key);
                    is_full_slice = false;
                    break;
                }
            }

            if !is_full_slice {
                break;
            }

            let first_header_block_num = slice_header_keys[0].0;

            let slice = HeaderSlice {
                start_block_num: first_header_block_num,
                status: HeaderSliceStatus::Saved,
                headers: Some(headers),
                ..Default::default()
            };

            slices.push(slice);
        }

        let header_slices =
            HeaderSlices::from_slices_vec(slices, Some(start_block_num), Some(max_slices), None);
        Ok(header_slices)
    }

    fn downloaded_count(header_slices: &HeaderSlices, start_block_num: BlockNumber) -> usize {
        let mut count = 0;
        header_slices.try_fold((), |_, slice_lock| {
            let slice = slice_lock.read();

            if slice.status == HeaderSliceStatus::Saved {
                let slice_range = slice.block_num_range();
                // if start_block_num is in the middle of the slice - ignore blocks before it
                if slice_range.contains(&start_block_num) {
                    count += slice.len() - (start_block_num.0 - slice_range.start.0) as usize;
                } else if slice.start_block_num > start_block_num {
                    count += slice.len();
                }
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        });
        count
    }

    fn build_stages<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        group_name: &str,
        stages: &mut DownloaderStageLoop<'downloader>,
        header_slices: &Arc<HeaderSlices>,
        save_stage: SaveStage<'downloader, RwTx>,
    ) {
        let sentry = self.sentry.clone();

        let fetch_request_stage = FetchRequestStage::new(
            header_slices.clone(),
            sentry.clone(),
            header_slices::HEADER_SLICE_SIZE,
        );
        let fetch_receive_stage = FetchReceiveStage::new(header_slices.clone(), sentry.clone());
        let retry_stage = RetryStage::new(header_slices.clone());
        let verify_slices_stage = VerifySlicesStage::new(
            header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
        );
        let refetch_stage = RefetchStage::new(header_slices.clone());
        let penalize_stage = PenalizeStage::new(header_slices.clone(), sentry);

        stages.insert_with_group_name(fetch_request_stage, group_name);
        stages.insert_with_group_name(fetch_receive_stage, group_name);
        stages.insert_with_group_name(retry_stage, group_name);
        stages.insert_with_group_name(verify_slices_stage, group_name);
        stages.insert_with_group_name(refetch_stage, group_name);
        stages.insert_with_group_name(penalize_stage, group_name);
        stages.insert_with_group_name(save_stage, group_name);
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        progress_start_block_num: BlockNumber,
        max_blocks_count: usize,
        no_forks_final_block_num: BlockNumber,
        previous_run_header_slices: Option<Arc<HeaderSlices>>,
        previous_run_fork_header_slices: Option<Arc<HeaderSlices>>,
        ui_system: UISystemShared,
    ) -> anyhow::Result<DownloaderForkyReport> {
        // don't use previous_run_header_slices if progress_start_block_num is before its range
        let previous_run_header_slices = previous_run_header_slices
            .filter(|slices| (progress_start_block_num >= slices.min_block_num()));
        let previous_run_fork_header_slices =
            previous_run_fork_header_slices.filter(|_| previous_run_header_slices.is_some());

        // header slices window start block number
        let start_block_num = if let Some(previous_run_header_slices) = &previous_run_header_slices
        {
            previous_run_header_slices.min_block_num()
        } else {
            no_forks_final_block_num
        };
        if !is_block_num_aligned_to_slice_start(start_block_num) {
            return Err(anyhow::format_err!(
                "expected an aligned start block, got {}",
                start_block_num.0
            ));
        }

        // Assuming we've downloaded all but last 90K headers in previous phases
        // we need to download them now, plus a bit more,
        // because extra blocks have been generating while downloading.
        // (ropsten/mainnet generate about 6500K blocks per day, and the sync is hopefully faster)
        // It must be less than Opts::headers_batch_size to pass the max_blocks_count check below.
        let forky_max_blocks_count: usize = 99_000;

        if (max_blocks_count < forky_max_blocks_count)
            || (progress_start_block_num < start_block_num)
        {
            return Ok(DownloaderForkyReport {
                loaded_count: 0,
                final_block_num: progress_start_block_num,
                header_slices: previous_run_header_slices,
                fork_header_slices: previous_run_fork_header_slices,
            });
        }

        // load start block parent header
        let start_block_parent_num = if start_block_num.0 > 0 {
            Some(BlockNumber(start_block_num.0 - 1))
        } else {
            None
        };
        let start_block_parent_header = match start_block_parent_num {
            Some(num) => SaveStage::load_canonical_header_by_num(num, db_transaction).await?,
            None => None,
        };
        if start_block_parent_num.is_some() && start_block_parent_header.is_none() {
            anyhow::bail!("expected a saved parent header of {}", start_block_num.0);
        }

        let header_slices = if let Some(previous_run_header_slices) = previous_run_header_slices {
            previous_run_header_slices
        } else {
            let loaded_header_slices =
                Self::load_header_slices(db_transaction, start_block_num, forky_max_blocks_count)
                    .await?;
            Arc::new(loaded_header_slices)
        };

        let fork_header_slices = previous_run_fork_header_slices
            .unwrap_or_else(|| Arc::new(HeaderSlices::empty(header_slices.max_slices())));

        let header_slices_view = HeaderSlicesView::new(header_slices.clone(), "DownloaderForky");
        let _header_slices_view_scope =
            UISystemViewScope::new(&ui_system, Box::new(header_slices_view));

        let verify_link_stage = VerifyLinkForkyStage::new(
            header_slices.clone(),
            fork_header_slices.clone(),
            self.chain_config.clone(),
            self.verifier.clone(),
            start_block_parent_header,
        );

        let save_stage = SaveStage::<RwTx>::new(
            header_slices.clone(),
            db_transaction,
            save_stage::SaveOrder::Monotonic,
            true,
        );

        let fork_save_stage = SaveStage::<RwTx>::new(
            fork_header_slices.clone(),
            db_transaction,
            save_stage::SaveOrder::Random,
            false,
        );

        let timeout_stage = TimeoutStage::new(Duration::from_secs(15));
        let timeout_stage_is_over = timeout_stage.is_over_check();

        let mut stages = DownloaderStageLoop::new(&header_slices, Some(&fork_header_slices));

        self.build_stages("main", &mut stages, &header_slices, save_stage);
        self.build_stages("fork", &mut stages, &fork_header_slices, fork_save_stage);

        // verify_link_stage is common for both groups
        stages.insert(verify_link_stage);
        stages.insert(timeout_stage);

        stages.run(timeout_stage_is_over).await;

        let loaded_count = Self::downloaded_count(header_slices.as_ref(), progress_start_block_num);

        let report = DownloaderForkyReport {
            loaded_count,
            final_block_num: BlockNumber(progress_start_block_num.0 + loaded_count as u64),
            header_slices: Some(header_slices),
            fork_header_slices: Some(fork_header_slices),
        };

        Ok(report)
    }
}
