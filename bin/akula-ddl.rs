use akula::downloader::{chain_config, opts::Opts, Downloader};

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let chains_config = chain_config::ChainsConfig::new()?;
    let opts = Opts::new(None, chains_config.chain_names().as_slice())?;
    let downloader = Downloader::new(opts, chains_config);
    downloader.run(None).await
}
