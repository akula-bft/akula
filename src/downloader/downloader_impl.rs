use crate::{
    downloader::opts::Opts,
    kv,
    sentry::{
        chain_config::{ChainConfig, ChainsConfig},
        sentry_client,
        sentry_client::SentryClient,
        sentry_client_connector,
        sentry_client_reactor::SentryClientReactor,
    },
};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct Downloader<DB: kv::traits::MutableKV + Sync> {
    opts: Opts,
    chain_config: ChainConfig,
    db: Arc<DB>,
}

impl<DB: kv::traits::MutableKV + Sync> Downloader<DB> {
    pub fn new(opts: Opts, chains_config: ChainsConfig, db: Arc<DB>) -> anyhow::Result<Self> {
        let chain_config = chains_config
            .get(&opts.chain_name)
            .ok_or_else(|| anyhow::anyhow!("unknown chain '{}'", opts.chain_name))?
            .clone();

        Ok(Self {
            opts,
            chain_config,
            db,
        })
    }

    pub async fn run(
        &self,
        sentry_client_opt: Option<Box<dyn SentryClient>>,
    ) -> anyhow::Result<()> {
        let status = sentry_client::Status {
            total_difficulty: ethereum_types::U256::zero(),
            best_hash: ethereum_types::H256::zero(),
            chain_fork_config: self.chain_config.clone(),
            max_block: 0,
        };

        let sentry_api_addr = self.opts.sentry_api_addr.clone();
        let sentry_client = match sentry_client_opt {
            Some(mut test_client) => {
                test_client.set_status(status.clone()).await?;
                test_client
            }
            None => {
                sentry_client_connector::connect(sentry_api_addr.clone(), status.clone()).await?
            }
        };

        let sentry_connector =
            sentry_client_connector::make_connector_stream(sentry_client, sentry_api_addr, status);
        let mut sentry_reactor = SentryClientReactor::new(sentry_connector);
        sentry_reactor.start()?;
        let sentry = Arc::new(RwLock::new(sentry_reactor));

        let mut ui_system = crate::downloader::ui_system::UISystem::new();
        ui_system.start()?;
        let ui_system = Arc::new(Mutex::new(ui_system));

        let headers_downloader = super::headers::downloader::Downloader::new(
            self.chain_config.clone(),
            sentry.clone(),
            self.db.clone(),
            ui_system.clone(),
        );
        headers_downloader.run().await?;

        ui_system.try_lock()?.stop().await?;

        {
            let mut sentry_reactor = sentry.write().await;
            sentry_reactor.stop().await?;
        }

        Ok(())
    }
}
