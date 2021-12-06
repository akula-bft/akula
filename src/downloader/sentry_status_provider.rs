use crate::{
    kv,
    sentry::{chain_config::ChainConfig, sentry_client::Status, sentry_client_connector},
};
use tokio::sync::watch;
use tokio_stream::{wrappers::WatchStream, StreamExt};

#[derive(Debug)]
pub struct SentryStatusProvider {
    chain_config: ChainConfig,
    sender: watch::Sender<Status>,
}

impl SentryStatusProvider {
    pub fn new(chain_config: ChainConfig) -> Self {
        let genesis_status = Status {
            total_difficulty: ethereum_types::U256::zero(),
            best_hash: ethereum_types::H256::zero(),
            chain_fork_config: chain_config.clone(),
            max_block: 0,
        };

        let (sender, _) = watch::channel(genesis_status);

        Self {
            chain_config,
            sender,
        }
    }

    pub fn current_status_stream(&self) -> sentry_client_connector::StatusStream {
        let receiver = self.sender.subscribe();
        let stream = WatchStream::new(receiver).map(Ok);
        Box::pin(stream)
    }

    pub async fn update<'db, RwTx: kv::traits::MutableTransaction<'db>>(
        &self,
        _tx: &RwTx,
    ) -> anyhow::Result<()> {
        // TODO: read from tx and send a new status
        // self.sender.send(...);
        Ok(())
    }
}
