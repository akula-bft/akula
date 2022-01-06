use super::{
    downloader_forky, downloader_linear, downloader_preverified,
    header_slices::align_block_num_to_slice_start,
};
use crate::{
    downloader::ui_system::UISystemShared,
    kv,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, messages::BlockHashAndNumber, sentry_client_reactor::*},
};

#[derive(Debug)]
pub struct Downloader {
    downloader_preverified: downloader_preverified::DownloaderPreverified,
    downloader_linear: downloader_linear::DownloaderLinear,
    downloader_forky: downloader_forky::DownloaderForky,
    genesis_block_hash: ethereum_types::H256,
}

pub struct DownloaderReport {
    pub final_block_num: BlockNumber,
    pub target_final_block_num: BlockNumber,
    pub run_state: DownloaderRunState,
}

#[derive(Clone, Debug)]
pub struct DownloaderRunState {
    pub estimated_top_block_num: Option<BlockNumber>,
}

impl Downloader {
    pub fn new(
        chain_config: ChainConfig,
        mem_limit: usize,
        sentry: SentryClientReactorShared,
    ) -> anyhow::Result<Self> {
        let downloader_preverified = downloader_preverified::DownloaderPreverified::new(
            chain_config.chain_name(),
            mem_limit,
            sentry.clone(),
        )?;

        let downloader_linear = downloader_linear::DownloaderLinear::new(
            chain_config.clone(),
            mem_limit,
            sentry.clone(),
        );

        let downloader_forky = downloader_forky::DownloaderForky::new(chain_config.clone(), sentry);

        let instance = Self {
            downloader_preverified,
            downloader_linear,
            downloader_forky,
            genesis_block_hash: chain_config.genesis_block_hash(),
        };
        Ok(instance)
    }

    async fn linear_start_block_id<
        'downloader,
        'db: 'downloader,
        RwTx: kv::traits::MutableTransaction<'db>,
    >(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        prev_final_block_num: BlockNumber,
    ) -> anyhow::Result<BlockHashAndNumber> {
        // start from one slice back where the hash is known
        let linear_start_block_num = if prev_final_block_num.0 > 0 {
            align_block_num_to_slice_start(BlockNumber(prev_final_block_num.0 - 1))
        } else {
            BlockNumber(0)
        };

        let linear_start_block_hash = if linear_start_block_num.0 > 0 {
            let hash_opt = db_transaction
                .get(kv::tables::CanonicalHeader, linear_start_block_num)
                .await?;
            hash_opt.ok_or_else(|| {
                anyhow::format_err!("Downloader inconsistent state: reported done until header {}, but header {} hash not found.",
                    prev_final_block_num.0,
                    linear_start_block_num.0)
            })?
        } else {
            self.genesis_block_hash
        };

        let linear_start_block_id = BlockHashAndNumber {
            number: linear_start_block_num,
            hash: linear_start_block_hash,
        };
        Ok(linear_start_block_id)
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

        let linear_start_block_id = self
            .linear_start_block_id(db_transaction, preverified_report.final_block_num)
            .await?;
        let linear_estimated_top_block_num = preverified_report
            .estimated_top_block_num
            .or_else(|| previous_run_state.and_then(|state| state.estimated_top_block_num));

        let linear_report = self
            .downloader_linear
            .run::<RwTx>(
                db_transaction,
                linear_start_block_id,
                linear_estimated_top_block_num,
                max_blocks_count,
                ui_system.clone(),
            )
            .await?;
        max_blocks_count -= linear_report.loaded_count;

        let forky_start_block_id = self
            .linear_start_block_id(db_transaction, linear_report.final_block_num)
            .await?;
        let forky_report = self
            .downloader_forky
            .run::<RwTx>(
                db_transaction,
                forky_start_block_id,
                max_blocks_count,
                ui_system,
            )
            .await?;
        // max_blocks_count -= forky_report.loaded_count;

        let report = DownloaderReport {
            final_block_num: forky_report.final_block_num,
            target_final_block_num: linear_report.target_final_block_num,
            run_state: DownloaderRunState {
                estimated_top_block_num: Some(linear_report.estimated_top_block_num),
            },
        };

        Ok(report)
    }
}
