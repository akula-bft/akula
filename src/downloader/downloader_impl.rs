use crate::downloader::{
    block_id,
    chain_config::{ChainConfig, ChainsConfig},
    messages::{EthMessageId, GetBlockHeadersMessage, GetBlockHeadersMessageParams, Message},
    opts::Opts,
    sentry_client,
    sentry_client::{PeerFilter, SentryClient},
    sentry_client_impl::SentryClientImpl,
    sentry_client_reactor::SentryClientReactor,
};
use tokio_stream::StreamExt;

pub struct Downloader {
    opts: Opts,
    chain_config: ChainConfig,
}

impl Downloader {
    pub fn new(opts: Opts, chains_config: ChainsConfig) -> Downloader {
        let chain_config = chains_config.0[&opts.chain_name].clone();

        Downloader { opts, chain_config }
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

        let mut sentry_client = match sentry_client_opt {
            Some(v) => v,
            None => Box::new(SentryClientImpl::new(self.opts.sentry_api_addr.clone()).await?),
        };

        sentry_client.set_status(status).await?;

        let mut sentry = SentryClientReactor::new(sentry_client);
        sentry.start();

        let message = Message::GetBlockHeaders(GetBlockHeadersMessage {
            request_id: 1,
            params: GetBlockHeadersMessageParams {
                start_block: block_id::BlockId::Number(123),
                limit: 5,
                skip: 0,
                reverse: 0,
            },
        });
        sentry.send_message(message, PeerFilter::All)?;

        let mut stream = sentry.receive_messages(EthMessageId::BlockHeaders)?;
        while let Some(message) = stream.next().await {
            tracing::info!("incoming message: {:?}", message.eth_id());
        }

        sentry.stop().await?;

        Ok(())
    }
}
