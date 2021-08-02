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

#[tokio::test]
async fn noop() {
    let sentry = SentryClientMock::new();

    let downloader = make_downloader();
    downloader.run(Some(Box::new(sentry))).await.unwrap();
}
