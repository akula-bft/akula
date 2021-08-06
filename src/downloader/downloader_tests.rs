use crate::downloader::{
    chain_config, opts::Opts, sentry_client_mock::SentryClientMock, Downloader,
};

fn make_downloader() -> Downloader {
    let chains_config = chain_config::ChainsConfig::new().unwrap();
    let args = Vec::<String>::new();
    let opts = Opts::new(Some(args), chains_config.chain_names().as_slice()).unwrap();
    let downloader = Downloader::new(opts, chains_config);
    let _ = downloader;
    downloader
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

    let downloader = make_downloader();
    downloader.run(Some(Box::new(sentry))).await.unwrap();
}
