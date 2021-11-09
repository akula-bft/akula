use crate::{
    downloader::{opts::Opts, Downloader},
    kv, new_mem_database,
    sentry::{chain_config, sentry_client_mock::SentryClientMock},
};
use std::sync::Arc;

fn make_downloader() -> Downloader<impl kv::traits::MutableKV> {
    let chains_config = chain_config::ChainsConfig::new().unwrap();
    let args = Vec::<String>::new();
    let opts = Opts::new(Some(args), chains_config.chain_names().as_slice()).unwrap();
    let db = Arc::new(new_mem_database().unwrap());
    let downloader = Downloader::new(opts, chains_config, db);
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
