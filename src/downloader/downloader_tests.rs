use crate::{
    downloader::{
        headers::downloader::DownloaderReport, sentry_status_provider::SentryStatusProvider,
        Downloader,
    },
    kv,
    kv::traits::*,
    models::BlockNumber,
    sentry::{
        chain_config, sentry_client_connector,
        sentry_client_connector::SentryClientConnectorTest,
        sentry_client_mock::SentryClientMock,
        sentry_client_reactor::{SentryClientReactor, SentryClientReactorShared},
    },
};

fn make_chain_config() -> chain_config::ChainConfig {
    let chains_config = chain_config::ChainsConfig::new().unwrap();
    let chain_name = "mainnet";
    chains_config.get(chain_name).unwrap()
}

fn make_sentry_reactor(
    sentry: SentryClientMock,
    current_status_stream: sentry_client_connector::StatusStream,
) -> SentryClientReactorShared {
    let sentry_connector = Box::new(SentryClientConnectorTest::new(Box::new(sentry)));
    let sentry_reactor = SentryClientReactor::new(sentry_connector, current_status_stream);
    sentry_reactor.into_shared()
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
    )
    .unwrap();
    run_downloader(downloader, sentry_reactor).await.unwrap();
}
