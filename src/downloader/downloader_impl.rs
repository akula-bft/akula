use crate::downloader::{
    block_id,
    chain_config::{ChainConfig, ChainsConfig},
    messages,
    opts::Opts,
    sentry_client,
    sentry_client::{PeerFilter, SentryClient},
    sentry_client_impl::SentryClientImpl,
};

use tokio_stream::StreamExt;
use tracing::*;

pub struct Downloader {
    opts: Opts,
    chain_config: ChainConfig,
}

impl Downloader {
    pub fn new(opts: Opts, chains_config: ChainsConfig) -> Downloader {
        let chain_config = chains_config.0[&opts.chain_name].clone();

        Downloader { opts, chain_config }
    }

    pub async fn run(&self, sentry_opt: Option<Box<dyn SentryClient>>) -> anyhow::Result<()> {
        let status = sentry_client::Status {
            total_difficulty: ethereum_types::U256::zero(),
            best_hash: ethereum_types::H256::zero(),
            chain_fork_config: self.chain_config.clone(),
            max_block: 0,
        };

        let mut sentry = match sentry_opt {
            Some(v) => v,
            None => Box::new(SentryClientImpl::new(self.opts.sentry_api_addr.clone()).await?),
        };

        sentry.set_status(status).await?;

        let message = messages::Message::GetBlockHeaders(messages::GetBlockHeadersMessage {
            request_id: 0,
            start_block: block_id::BlockId::Number(0),
            limit: 0,
            skip: 0,
            reverse: false,
        });
        sentry.send_message(message, PeerFilter::All).await?;

        let mut stream = sentry.receive_messages().await?;
        while let Some(message_result) = stream.next().await {
            match message_result {
                Ok(message_from_peer) => self.handle_incoming_message(&message_from_peer.message),
                Err(error) => {
                    error!("receive message error {}", error);
                }
            }
        }

        Ok(())
    }

    fn handle_incoming_message(&self, message: &messages::Message) {
        tracing::info!("incoming message: {:?}", message.eth_id());
    }
}
