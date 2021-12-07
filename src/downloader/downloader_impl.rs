use super::sentry_status_provider::SentryStatusProvider;
use crate::{
    downloader::headers::downloader::DownloaderReport,
    kv,
    models::BlockNumber,
    sentry::{chain_config::ChainConfig, sentry_client_reactor::SentryClientReactorShared},
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Downloader {
    chain_config: ChainConfig,
    sentry: SentryClientReactorShared,
    sentry_status_provider: SentryStatusProvider,
}

impl Downloader {
    pub fn new(
        chain_config: ChainConfig,
        sentry: SentryClientReactorShared,
        sentry_status_provider: SentryStatusProvider,
    ) -> Self {
        Self {
            chain_config,
            sentry,
            sentry_status_provider,
        }
    }

    pub async fn run<'downloader, 'db: 'downloader, RwTx: kv::traits::MutableTransaction<'db>>(
        &'downloader self,
        db_transaction: &'downloader RwTx,
        start_block_num: BlockNumber,
        max_blocks_count: usize,
    ) -> anyhow::Result<DownloaderReport> {
        self.sentry_status_provider.update(db_transaction).await?;

        let mut ui_system = crate::downloader::ui_system::UISystem::new();
        ui_system.start()?;
        let ui_system = Arc::new(Mutex::new(ui_system));

        let headers_downloader = super::headers::downloader::Downloader::new(
            self.chain_config.clone(),
            self.sentry.clone(),
            ui_system.clone(),
        )?;
        let report = headers_downloader
            .run::<RwTx>(db_transaction, start_block_num, max_blocks_count)
            .await?;

        ui_system.try_lock()?.stop().await?;

        Ok(report)
    }
}
