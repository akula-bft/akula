use super::chain_config::ChainConfig;
use crate::downloader::sentry_address::SentryAddress;
use ethereum_interfaces::{
    self,
    sentry::{sentry_client, Forks, SetStatusReply, StatusData},
};
use tracing::*;

pub struct SentryClient {
    client: sentry_client::SentryClient<tonic::transport::channel::Channel>,
}

pub struct Status {
    pub total_difficulty: ethereum_types::U256,
    pub best_hash: ethereum_types::H256,
    pub chain_fork_config: ChainConfig,
    pub max_block: u64,
}

impl SentryClient {
    pub async fn new(addr: SentryAddress) -> anyhow::Result<Self> {
        info!("SentryClient connecting to {}...", addr.addr);
        let client = sentry_client::SentryClient::connect(addr.addr).await?;
        Ok(SentryClient { client })
    }

    pub async fn set_status(&mut self, status: Status) -> anyhow::Result<()> {
        let fork_data = Forks {
            genesis: Some(status.chain_fork_config.genesis_block_hash.into()),
            forks: status.chain_fork_config.fork_block_numbers,
        };

        let status_data = StatusData {
            network_id: u64::from(status.chain_fork_config.id.0),
            total_difficulty: Some(ethereum_interfaces::types::H256::from(
                status.total_difficulty,
            )),
            best_hash: Some(ethereum_interfaces::types::H256::from(status.best_hash)),
            fork_data: Some(fork_data),
            max_block: status.max_block,
        };
        let request = tonic::Request::new(status_data);
        let response = self.client.set_status(request).await?;
        let reply: SetStatusReply = response.into_inner();
        debug!("SentryClient set_status replied with: {:?}", reply);
        Ok(())
    }
}
