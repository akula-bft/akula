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

pub struct Downloader<DB: kv::traits::MutableKV + Sync> {
    chain_config: ChainConfig,
    sentry: SentryClientReactorShared,
    db: Arc<DB>,
    ui_system: Arc<Mutex<UISystem>>,
}

impl<DB: kv::traits::MutableKV + Sync> Downloader<DB> {
    pub fn new(
        chain_config: ChainConfig,
        sentry: SentryClientReactorShared,
        db: Arc<DB>,
        ui_system: Arc<Mutex<UISystem>>,
    ) -> Self {
        Self {
            chain_config,
            sentry,
            db,
            ui_system,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mem_limit = 50 << 20; /* 50 Mb */

        let downloader_preverified = downloader_preverified::DownloaderPreverified::new(
            self.chain_config.chain_name(),
            mem_limit,
            self.sentry.clone(),
            self.db.clone(),
            self.ui_system.clone(),
        );

        let preverified_report = downloader_preverified.run().await?;

        let downloader_linear = downloader_linear::DownloaderLinear::new(
            self.chain_config.clone(),
            preverified_report.final_block_id,
            preverified_report.estimated_top_block_num,
            mem_limit,
            self.sentry.clone(),
            self.db.clone(),
            self.ui_system.clone(),
        );

        downloader_linear.run().await?;

        Ok(())
    }
}
