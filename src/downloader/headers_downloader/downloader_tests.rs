use super::{
    super::sentry_status_provider::SentryStatusProvider,
    downloader::{Downloader, DownloaderReport, DownloaderRunState},
    headers::{
        header::BlockHeader,
        header_slices,
        header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    },
    ui::ui_system::UISystem,
    verification::header_slice_verifier_mock::HeaderSliceVerifierMock,
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
use bytes::{Buf, BufMut, BytesMut};
use std::{
    mem::size_of,
    sync::{Arc, Once},
};
use tokio::sync::Mutex as AsyncMutex;

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

    let ui_system = Arc::new(AsyncMutex::new(UISystem::new()));

    let report = downloader
        .run(
            &db_transaction,
            BlockNumber(0),
            100_000,
            previous_run_state,
            ui_system,
        )
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
        chain_config: chain_config::ChainConfig,
        sentry: SentryClientMock,
        verifier: HeaderSliceVerifierMock,
        previous_run_state: Option<DownloaderRunState>,
        expected_report: Option<DownloaderReport>,
    ) -> anyhow::Result<Self> {
        setup_logging_once();

        let status_provider = SentryStatusProvider::new(chain_config.clone());
        let sentry_reactor = make_sentry_reactor(sentry, status_provider.current_status_stream());
        let downloader = Downloader::new(
            chain_config,
            Box::new(verifier),
            byte_unit::n_mib_bytes!(50) as usize,
            sentry_reactor.clone(),
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

        let actual_forky_fork_header_slices = actual_report.run_state.forky_fork_header_slices;
        let expected_forky_fork_header_slices = expected_report.run_state.forky_fork_header_slices;
        Self::check_forky_header_slices(
            actual_forky_fork_header_slices,
            expected_forky_fork_header_slices,
        );
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
    let chain_config = make_chain_config();
    let sentry = SentryClientMock::new();
    let verifier = HeaderSliceVerifierMock::new(HeaderGenerator::header_id);
    let test = DownloaderTest::new(chain_config, sentry, verifier, None, None).unwrap();
    test.run().await.unwrap();
}

struct DownloaderTestDecl<'t> {
    // SentryClientMock descriptor
    pub sentry: &'t str,
    // header slices descriptor at a previous downloader run
    pub slices: &'t str,
    // expected header slices descriptor after the test
    pub result: &'t str,
    // expected forky phase fork header slices descriptor after the test
    pub forked: &'t str,
}

impl<'t> DownloaderTestDecl<'t> {
    fn into_test(self) -> anyhow::Result<DownloaderTest> {
        let chain_config = make_chain_config();
        let mut generator = HeaderGenerator::new(chain_config.clone());

        let sentry = Self::parse_sentry(self.sentry, &mut generator)?;

        let (forky_header_slices, verifier) = Self::parse_slices(self.slices, &mut generator)?;
        let forky_header_slices = HeaderSlices::from_slices_vec(forky_header_slices, 100);

        let previous_run_state = DownloaderRunState {
            estimated_top_block_num: Some(BlockNumber(10_000)),
            forky_header_slices: Some(Arc::new(forky_header_slices)),
            forky_fork_header_slices: None,
        };

        let expected_forky_header_slices =
            HeaderSlices::from_slices_vec(Self::parse_slices(self.result, &mut generator)?.0, 100);

        let expected_forky_fork_header_slices =
            HeaderSlices::from_slices_vec(Self::parse_slices(self.forked, &mut generator)?.0, 100);

        let expected_report = DownloaderReport {
            final_block_num: BlockNumber(0),
            target_final_block_num: BlockNumber(0),
            run_state: DownloaderRunState {
                estimated_top_block_num: Some(BlockNumber(10_000)),
                forky_header_slices: Some(Arc::new(expected_forky_header_slices)),
                forky_fork_header_slices: Some(Arc::new(expected_forky_fork_header_slices)),
            },
        };

        DownloaderTest::new(
            chain_config,
            sentry,
            verifier,
            Some(previous_run_state),
            Some(expected_report),
        )
    }

    fn parse_sentry(
        desc: &str,
        generator: &mut HeaderGenerator,
    ) -> anyhow::Result<SentryClientMock> {
        let mut sentry = SentryClientMock::new();
        let mut start_block_num = BlockNumber(0);

        for c in desc.chars() {
            if c == ' ' {
                continue;
            }
            let status = HeaderSliceStatus::try_from(c)?;

            if let Some(headers) =
                generator.generate_slice_headers_if_needed(status, start_block_num)
            {
                let raw_headers = headers.into_iter().map(|header| header.header).collect();
                sentry.add_block_headers(raw_headers);
            }

            start_block_num =
                BlockNumber(start_block_num.0 + header_slices::HEADER_SLICE_SIZE as u64);
        }

        Ok(sentry)
    }

    fn parse_slices(
        desc: &str,
        generator: &mut HeaderGenerator,
    ) -> anyhow::Result<(Vec<HeaderSlice>, HeaderSliceVerifierMock)> {
        let mut slices = Vec::<HeaderSlice>::new();
        let mut verifier = HeaderSliceVerifierMock::new(HeaderGenerator::header_id);
        let mut start_block_num = BlockNumber(0);
        let mut is_link_broken = false;

        for c in desc.chars() {
            match c {
                ' ' => continue,
                '|' => {
                    is_link_broken = true;
                    continue;
                }
                '_' => {
                    start_block_num =
                        BlockNumber(start_block_num.0 + header_slices::HEADER_SLICE_SIZE as u64);
                    continue;
                }
                _ => (),
            }
            let status = HeaderSliceStatus::try_from(c)?;

            let mut slice = HeaderSlice {
                start_block_num,
                status,
                headers: generator.generate_slice_headers_if_needed(status, start_block_num),
                ..Default::default()
            };

            if is_link_broken {
                let Some(headers) = slice.headers.as_mut() else {
                    anyhow::bail!("expected a status that has non empty headers");
                };
                if let Some(parent_slice) = slices.last_mut() {
                    let Some(parent_headers) = parent_slice.headers.as_mut() else {
                        anyhow::bail!("expected that a previous slice has non empty headers");
                    };

                    let child_header = headers.first_mut().unwrap();
                    let parent_header = parent_headers.last_mut().unwrap();
                    verifier.mark_broken_link(child_header, parent_header);
                } else {
                    // the genesis slice (with a block number 0)
                    // fails to link if its genesis header hash is wrong
                    if start_block_num == BlockNumber(0) {
                        headers[0].set_hash_cached(Some(ethereum_types::H256::zero()));
                    } else {
                        anyhow::bail!("expected to have a previous slice");
                    }
                }
                is_link_broken = false;
            }

            slices.push(slice);

            start_block_num =
                BlockNumber(start_block_num.0 + header_slices::HEADER_SLICE_SIZE as u64);
        }

        Ok((slices, verifier))
    }

    pub async fn run(self) -> anyhow::Result<()> {
        self.into_test()?.run().await
    }
}

struct HeaderGenerator {
    chain_config: chain_config::ChainConfig,
    last_id: u64,
}

impl HeaderGenerator {
    pub fn new(chain_config: chain_config::ChainConfig) -> Self {
        Self {
            chain_config,
            last_id: 0,
        }
    }

    fn next_id(&mut self) -> u64 {
        self.last_id += 1;
        self.last_id
    }

    fn mark_header_id(&self, header: &mut BlockHeader, id: u64) {
        let mut data = BytesMut::with_capacity(size_of::<u64>());
        data.put_u64(id);
        header.header.extra_data = data.freeze();
    }

    pub fn header_id(header: &BlockHeader) -> u64 {
        if header.header.extra_data.len() >= size_of::<u64>() {
            let mut data = header.header.extra_data.clone();
            data.get_u64()
        } else {
            0
        }
    }

    pub fn generate_slice_headers_if_needed(
        &mut self,
        status: HeaderSliceStatus,
        start_block_num: BlockNumber,
    ) -> Option<Vec<BlockHeader>> {
        match status {
            HeaderSliceStatus::Empty => None,
            HeaderSliceStatus::Waiting => None,
            HeaderSliceStatus::Refetch => None,
            _ => Some(self.generate_slice_headers(start_block_num)),
        }
    }

    pub fn generate_slice_headers(&mut self, start_block_num: BlockNumber) -> Vec<BlockHeader> {
        let header = BlockHeader::from(crate::models::BlockHeader::empty());
        let mut headers = vec![header; header_slices::HEADER_SLICE_SIZE];

        // set block numbers
        let mut num = start_block_num;
        for header in headers.as_mut_slice() {
            header.header.number = num;
            let id = self.next_id();
            self.mark_header_id(header, id);
            num = BlockNumber(num.0 + 1);
        }

        // set genesis header hash
        if start_block_num == BlockNumber(0) {
            headers[0].set_hash_cached(Some(self.chain_config.genesis_block_hash()));
        }

        headers
    }
}

#[tokio::test]
async fn save_verified() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "+   +   +   #",
        result: "+   +   +   +",
        forked: "",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn verify_link() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "+   +   +   =",
        result: "+   +   +   +",
        forked: "",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn verify_link_at_root() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "=",
        result: "+",
        forked: "",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn fork_mode_start() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "+   +   +   |=  ",
        result: "+   +   +   -",
        forked: "_   _   -   +",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn fork_continuation() {
    let test = DownloaderTestDecl {
        sentry: "-   -   .   -   ",
        slices: "+   +   +   |=  ",
        result: "+   +   +   -",
        forked: "_   -   +   +",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn canonical_continuation() {
    let test = DownloaderTestDecl {
        sentry: "-   -   -   .   -",
        slices: "+   +   +   |=  -",
        result: "+   +   +   +   -",
        forked: "_   _   -   +   _",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn canonical_continuation_to_top() {
    let test = DownloaderTestDecl {
        sentry: "-   -   -   .   ",
        slices: "+   +   +   |=  ",
        result: "+   +   +   +   ",
        forked: "_   _   -   +   ",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn fork_both_chains_continuation() {
    let test = DownloaderTestDecl {
        sentry: "-   -   .   .   ",
        slices: "+   +   +   |=  ",
        result: "+   +   +   +   ",
        forked: "_   -   +   +   ",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn fork_discarded_reaching_root() {
    let test = DownloaderTestDecl {
        sentry: ".   -   ",
        slices: "+   |=  ",
        result: "+   -   ",
        forked: "",
    };
    test.run().await.unwrap();
}

#[tokio::test]
async fn dont_fork_at_root() {
    let test = DownloaderTestDecl {
        sentry: "",
        slices: "|=  ",
        result: "-   ",
        forked: "",
    };
    test.run().await.unwrap();
}
