use crate::{
    downloader::{opts::Opts, Downloader},
    kv,
    kv::traits::{MutableKV, MutableTransaction},
    sentry::{chain_config, sentry_client_mock::SentryClientMock},
};

fn make_downloader() -> Downloader {
    let chains_config = chain_config::ChainsConfig::new().unwrap();
    let args = Vec::<String>::new();
    let opts = Opts::new(Some(args), chains_config.chain_names().as_slice()).unwrap();
    let downloader = Downloader::new(opts, chains_config).unwrap();
    let _ = downloader;
    downloader
}

async fn run_downloader(downloader: Downloader, sentry: SentryClientMock) -> anyhow::Result<()> {
    let db = kv::new_mem_database()?;
    let db_transaction = db.begin_mutable().await?;
    downloader
        .run(Some(Box::new(sentry)), &db_transaction)
        .await?;
    db_transaction.commit().await
}

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::test]
async fn noop() {
    setup_logging();

    let downloader = make_downloader();
    let sentry = SentryClientMock::new();
    run_downloader(downloader, sentry).await.unwrap();
}
