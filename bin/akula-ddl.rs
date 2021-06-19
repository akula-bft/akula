use akula::downloader::{
    block_id, chain_config, messages,
    opts::Opts,
    sentry_client,
    sentry_client::{PeerFilter, SentryClient},
    sentry_client_impl::SentryClientImpl,
};

use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::EnvFilter;

async fn run() -> anyhow::Result<()> {
    let chains_config = chain_config::ChainsConfig::new()?;
    let chain_names = chains_config
        .0
        .keys()
        .map(|k| k.as_str())
        .collect::<Vec<&str>>();
    let opts = Opts::new(chain_names.as_slice())?;
    let chain_config = chains_config.0[&opts.chain_name].clone();
    let status = sentry_client::Status {
        total_difficulty: ethereum_types::U256::zero(),
        best_hash: ethereum_types::H256::zero(),
        chain_fork_config: chain_config,
        max_block: 0,
    };

    let mut sentry = SentryClientImpl::new(opts.sentry_api_addr).await?;
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
            Ok(_) => (),
            Err(error) => error!("receive message error {}", error),
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    run().await
}
