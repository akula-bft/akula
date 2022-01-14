use super::{
    headers::{
        downloader::{DownloaderReport, DownloaderRunState},
        header::BlockHeader,
        header_slices,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    sentry_status_provider::SentryStatusProvider,
    Downloader,
};
use crate::{
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
use std::sync::{Arc, Once};

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
    previous_run_state: Option<DownloaderRunState>,
) -> anyhow::Result<DownloaderReport> {
    {
        sentry.write().await.start()?;
    }

    let db = kv::new_mem_database()?;
    let db_transaction = db.begin_mutable().await?;

    let report = downloader
        .run(&db_transaction, BlockNumber(0), 100_000, previous_run_state)
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

static SETUP_LOGGING_ONCE_TOKEN: Once = Once::new();

fn setup_logging_once() {
    SETUP_LOGGING_ONCE_TOKEN.call_once(|| {
        setup_logging();
    })
}

struct DownloaderTest {
    pub downloader: Downloader,
    pub sentry_reactor: SentryClientReactorShared,
    pub previous_run_state: Option<DownloaderRunState>,
    pub expected_report: Option<DownloaderReport>,
}

impl DownloaderTest {
    pub fn new(
        sentry: SentryClientMock,
        previous_run_state: Option<DownloaderRunState>,
        expected_report: Option<DownloaderReport>,
    ) -> anyhow::Result<Self> {
        setup_logging_once();

        let chain_config = make_chain_config();
        let status_provider = SentryStatusProvider::new(chain_config.clone());
        let sentry_reactor = make_sentry_reactor(sentry, status_provider.current_status_stream());
        let downloader = Downloader::new(
            chain_config,
            byte_unit::n_mib_bytes!(50) as usize,
            sentry_reactor.clone(),
            status_provider,
        )?;

        let instance = Self {
            downloader,
            sentry_reactor,
            previous_run_state,
            expected_report,
        };
        Ok(instance)
    }

    fn check(actual_report: DownloaderReport, expected_report: DownloaderReport) {
        let actual_forky_header_slices = actual_report.run_state.forky_header_slices;
        let expected_forky_header_slices = expected_report.run_state.forky_header_slices;
        Self::check_forky_header_slices(actual_forky_header_slices, expected_forky_header_slices);
    }

    fn check_forky_header_slices(
        actual_slices_opt: Option<Arc<HeaderSlices>>,
        expected_slices_opt: Option<Arc<HeaderSlices>>,
    ) {
        assert_eq!(actual_slices_opt.is_some(), expected_slices_opt.is_some());

        let Some(actual_slices) = actual_slices_opt else { return };
        let Some(expected_slices) = expected_slices_opt else { return };

        let mut actual_slices_mut = actual_slices.clone_slices_vec();
        Self::update_slices_waiting_to_empty(actual_slices_mut.as_mut_slice());
        let actual_slices = actual_slices_mut;
        let expected_slices = expected_slices.clone_slices_vec();

        // check statuses
        let actual_statuses = actual_slices
            .iter()
            .map(|slice| slice.status)
            .collect::<Vec<HeaderSliceStatus>>();
        let expected_statuses = expected_slices
            .iter()
            .map(|slice| slice.status)
            .collect::<Vec<HeaderSliceStatus>>();
        assert_eq!(actual_statuses, expected_statuses);

        // check fork statuses
        let actual_fork_statuses = actual_slices
            .iter()
            .map(|slice| slice.fork_status)
            .collect::<Vec<HeaderSliceStatus>>();
        let expected_fork_statuses = expected_slices
            .iter()
            .map(|slice| slice.fork_status)
            .collect::<Vec<HeaderSliceStatus>>();
        assert_eq!(actual_fork_statuses, expected_fork_statuses);
    }

    // Empty slices might become Waiting at random by the FetchRequestStage
    // (if it has a chance to execute and the reactor send queue is not full).
    // For the purpose of some tests this is not significant,
    // and they treat Waiting status same as Empty.
    fn update_slices_waiting_to_empty(slices: &mut [HeaderSlice]) {
        for slice in slices {
            if slice.status == HeaderSliceStatus::Waiting {
                slice.status = HeaderSliceStatus::Empty;
            }
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let report = run_downloader(
            self.downloader,
            self.sentry_reactor,
            self.previous_run_state,
        )
        .await?;

        if let Some(expected_report) = self.expected_report {
            Self::check(report, expected_report);
        }

        Ok(())
    }
}

#[tokio::test]
async fn noop() {
    let sentry = SentryClientMock::new();
    let test = DownloaderTest::new(sentry, None, None).unwrap();
    test.run().await.unwrap();
}

struct DownloaderTestDecl<'t> {
    // SentryClientMock descriptor
    pub sentry: &'t str,
    // header slices descriptor at a previous downloader run
    pub slices: &'t str,
    // expected header slices descriptor after the test
    pub result: &'t str,
}

impl<'t> DownloaderTestDecl<'t> {
    fn into_test(self) -> anyhow::Result<DownloaderTest> {
        let sentry = SentryClientMock::new();
        let forky_header_slices = HeaderSlices::from_slices_vec(Self::parse_slices(self.slices)?);

        let previous_run_state = DownloaderRunState {
            estimated_top_block_num: Some(BlockNumber(10_000)),
            forky_header_slices: Some(Arc::new(forky_header_slices)),
        };

        let expected_forky_header_slices =
            HeaderSlices::from_slices_vec(Self::parse_slices(self.result)?);
        let expected_report = DownloaderReport {
            final_block_num: BlockNumber(0),
            target_final_block_num: BlockNumber(0),
            run_state: DownloaderRunState {
                estimated_top_block_num: Some(BlockNumber(10_000)),
                forky_header_slices: Some(Arc::new(expected_forky_header_slices)),
            },
        };

        DownloaderTest::new(sentry, Some(previous_run_state), Some(expected_report))
    }

    fn parse_slices(desc: &str) -> anyhow::Result<Vec<HeaderSlice>> {
        let mut slices = Vec::<HeaderSlice>::new();
        let mut start_block_num = BlockNumber(0);
        let mut is_fork = false;

        for c in desc.chars() {
            match c {
                ' ' => continue,
                '|' => continue,
                '/' => {
                    is_fork = true;
                    continue;
                }
                _ => (),
            }
            let status = HeaderSliceStatus::try_from(c)?;

            if is_fork {
                let last_slice = slices.last_mut().ok_or_else(|| {
                    anyhow::format_err!("expected a fork status, but no slices are present")
                })?;
                last_slice.fork_status = status;
                last_slice.fork_headers = Self::generate_slice_headers_if_needed(status);
                is_fork = false;
                continue;
            }

            let slice = HeaderSlice {
                start_block_num,
                status,
                headers: Self::generate_slice_headers_if_needed(status),
                ..Default::default()
            };
            slices.push(slice);

            start_block_num =
                BlockNumber(start_block_num.0 + header_slices::HEADER_SLICE_SIZE as u64);
        }

        Ok(slices)
    }

    fn generate_slice_headers_if_needed(status: HeaderSliceStatus) -> Option<Vec<BlockHeader>> {
        match status {
            HeaderSliceStatus::Empty => None,
            HeaderSliceStatus::Waiting => None,
            HeaderSliceStatus::Refetch => None,
            _ => Some(Self::generate_slice_headers()),
        }
    }

    fn generate_slice_headers() -> Vec<BlockHeader> {
        let header = BlockHeader::from(crate::models::BlockHeader::empty());
        vec![header; header_slices::HEADER_SLICE_SIZE]
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.into_test()?.run().await
    }
}

#[tokio::test]
async fn fork_mode_start() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "+   +   +   |= ",
        result: "+   +   -/+ -/Y",
    };
    test.run().await.unwrap();
}
