use crate::{
    downloader::{
        headers::{downloader_linear, downloader_preverified},
        ui_system::UISystem,
    },
    kv,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::*},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Downloader {
    chain_config: ChainConfig,
    sentry: SentryClientReactorShared,
    ui_system: Arc<Mutex<UISystem>>,
}

impl Downloader {
    pub fn new(
        chain_config: ChainConfig,
        sentry: SentryClientReactorShared,
        ui_system: Arc<Mutex<UISystem>>,
    ) -> Self {
        Self {
            chain_config,
            sentry,
            ui_system,
        }
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
    ) -> anyhow::Result<()> {
        let mem_limit = 50 << 20; /* 50 Mb */

        let downloader_preverified = downloader_preverified::DownloaderPreverified::new(
            self.chain_config.chain_name(),
            mem_limit,
            self.sentry.clone(),
            self.ui_system.clone(),
        );

        let preverified_report = downloader_preverified.run::<RwTx>(db_transaction).await?;

        let downloader_linear = downloader_linear::DownloaderLinear::new(
            self.chain_config.clone(),
            preverified_report.final_block_id,
            preverified_report.estimated_top_block_num,
            mem_limit,
            self.sentry.clone(),
            self.ui_system.clone(),
        );

        downloader_linear.run::<RwTx>(db_transaction).await?;

        Ok(())
    }
}
