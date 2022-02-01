use crate::{
    kv,
    kv::tables::HeaderKey,
    models::*,
    sentry::{chain_config::ChainConfig, sentry_client::Status, sentry_client_connector},
};
use std::fmt;
use tokio::sync::watch;
use tracing::debug;

#[derive(Debug)]
pub struct SentryStatusProvider {
    chain_config: ChainConfig,
    sender: watch::Sender<Status>,
}

impl SentryStatusProvider {
    pub fn new(chain_config: ChainConfig) -> Self {
        let genesis_status = Status {
            total_difficulty: U256::ZERO,
            best_hash: H256::zero(),
            chain_fork_config: chain_config.clone(),
            max_block: BlockNumber(0),
        };

        let (sender, _) = watch::channel(genesis_status);

        Self {
            chain_config,
            sender,
        }
    }

    pub fn current_status_stream(&self) -> sentry_client_connector::StatusStream {
        let receiver = self.sender.subscribe();
        let stream = async_stream::stream! {
            // move receiver
            let mut receiver: watch::Receiver<Status> = receiver;
            loop {
                let status = receiver.borrow_and_update().clone();
                yield Ok(status);
            }
        };
        Box::pin(stream)
    }

    async fn read_status<'db, RwTx: kv::traits::Transaction<'db>>(
        &self,
        tx: &RwTx,
    ) -> anyhow::Result<Status> {
        let header_hash = tx
            .get(kv::tables::LastHeader, Default::default())
            .await?
            .ok_or(SentryStatusProviderError::StatusDataNotFound)?;

        let block_num = tx
            .get(kv::tables::HeaderNumber, header_hash)
            .await?
            .ok_or(SentryStatusProviderError::StatusDataNotFound)?;

        let header_key: HeaderKey = (block_num, header_hash);
        let total_difficulty = tx
            .get(kv::tables::HeadersTotalDifficulty, header_key)
            .await?
            .ok_or(SentryStatusProviderError::StatusDataNotFound)?;

        let status = Status {
            total_difficulty,
            best_hash: header_hash,
            chain_fork_config: self.chain_config.clone(),
            max_block: block_num,
        };

        Ok(status)
    }

    pub async fn update<'db, RwTx: kv::traits::Transaction<'db>>(
        &self,
        tx: &RwTx,
    ) -> anyhow::Result<()> {
        let result = self.read_status(tx).await;

        match result {
            Ok(status) => {
                self.sender.send(status)?;
                Ok(())
            }
            Err(error) => match error.downcast_ref::<SentryStatusProviderError>() {
                Some(SentryStatusProviderError::StatusDataNotFound) => {
                    debug!("SentryStatusProvider.update: status data not found.");
                    Ok(())
                }
                None => Err(error),
            },
        }
    }
}

#[derive(Debug)]
enum SentryStatusProviderError {
    StatusDataNotFound,
}

impl fmt::Display for SentryStatusProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for SentryStatusProviderError {}
