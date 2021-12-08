use crate::{
    downloader::{
        headers::downloader::DownloaderReport, opts::Opts,
        sentry_status_provider::SentryStatusProvider, Downloader,
    },
    kv,
    kv::traits::{MutableKV, MutableTransaction},
    models::BlockNumber,
    sentry::{
        chain_config, sentry_client_connector,
        sentry_client_connector::SentryClientConnectorTest,
        sentry_client_mock::SentryClientMock,
        sentry_client_reactor::{SentryClientReactor, SentryClientReactorShared},
    },
};
use std::sync::Arc;
use tokio::sync::RwLock;

fn make_chain_config() -> chain_config::ChainConfig {
    let chains_config = chain_config::ChainsConfig::new().unwrap();
    let args = Vec::<String>::new();
    let opts = Opts::new(Some(args), chains_config.chain_names().as_slice()).unwrap();
    chains_config.get(&opts.chain_name).unwrap().clone()
}

fn make_sentry_reactor(
    sentry: SentryClientMock,
    current_status_stream: sentry_client_connector::StatusStream,
) -> SentryClientReactorShared {
    let sentry_connector = Box::new(SentryClientConnectorTest::new(Box::new(sentry)));
    let sentry_reactor = SentryClientReactor::new(sentry_connector, current_status_stream);
    Arc::new(RwLock::new(sentry_reactor))
}

async fn run_downloader(
    downloader: Downloader,
    sentry: SentryClientReactorShared,
) -> anyhow::Result<DownloaderReport> {
    {
        sentry.write().await.start()?;
    }

    let db = kv::new_mem_database()?;
    let db_transaction = db.begin_mutable().await?;

    let report = downloader
        .run(&db_transaction, BlockNumber(0), 100_000, None)
        .await?;

    db_transaction.commit().await?;

    {
        sentry.write().await.stop().await?;
    }
    Ok(report)
}

fn setup_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
}

#[tokio::test]
async fn noop() {
    setup_logging();

    let sentry = SentryClientMock::new();

    let chain_config = make_chain_config();
    let status_provider = SentryStatusProvider::new(chain_config.clone());
    let sentry_reactor = make_sentry_reactor(sentry, status_provider.current_status_stream());
    let downloader = Downloader::new(
        chain_config,
        byte_unit::n_mib_bytes!(50) as usize,
        sentry_reactor.clone(),
        status_provider,
    );
    run_downloader(downloader, sentry_reactor).await.unwrap();
}
