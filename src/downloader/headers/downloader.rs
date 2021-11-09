use crate::{
    downloader::{
        headers::{downloader_linear, downloader_preverified},
        ui_system::UISystem,
    },
    kv,
    sentry::sentry_client_reactor::SentryClientReactor,
};
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Downloader<DB: kv::traits::MutableKV + Sync> {
    chain_name: String,
    sentry: Arc<RwLock<SentryClientReactor>>,
    db: Arc<DB>,
    ui_system: Arc<Mutex<UISystem>>,
}

impl<DB: kv::traits::MutableKV + Sync> Downloader<DB> {
    pub fn new(
        chain_name: String,
        sentry: Arc<RwLock<SentryClientReactor>>,
        db: Arc<DB>,
        ui_system: Arc<Mutex<UISystem>>,
    ) -> Self {
        Self {
            chain_name,
            sentry,
            db,
            ui_system,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let mem_limit = 50 << 20; /* 50 Mb */

        let downloader_preverified = downloader_preverified::DownloaderPreverified::new(
            self.chain_name.clone(),
            mem_limit,
            self.sentry.clone(),
            self.db.clone(),
            self.ui_system.clone(),
        );

        let final_preverified_block_num = downloader_preverified.run().await?;

        let _downloader_linear = downloader_linear::DownloaderLinear::new(
            self.chain_name.clone(),
            final_preverified_block_num,
            mem_limit,
            self.sentry.clone(),
            self.db.clone(),
            self.ui_system.clone(),
        );

        //downloader_linear.run().await?;

        Ok(())
    }
}
