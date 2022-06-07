use akula::{
    akula_tracing::{self, Component},
    binutil::AkulaDataDir,
    consensus::{engine_factory, Consensus},
    models::*,
    p2p::node::NodeBuilder,
    rpc::{
        erigon::ErigonApiServerImpl, eth::EthApiServerImpl, net::NetApiServerImpl,
        otterscan::OtterscanApiServerImpl,
    },
    stagedsync::{
        self,
        stages::{BODIES, HEADERS},
    },
    stages::*,
    version_string,
};
use anyhow::Context;
use clap::Parser;
use ethereum_jsonrpc::{ErigonApiServer, EthApiServer, NetApiServer, OtterscanApiServer};
use http::Uri;
use jsonrpsee::{core::server::rpc_module::Methods, http_server::HttpServerBuilder};
use std::{
    fs::File, future::pending, net::SocketAddr, panic, path::PathBuf, sync::Arc, time::Duration,
};
use tokio::time::sleep;
use tracing::*;
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[clap(name = "Akula", about = "Next-generation Ethereum implementation.")]
pub struct Opt {
    /// Path to database directory.
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,

    /// Name of the network to join
    #[clap(long)]
    pub chain: Option<String>,

    /// Chain spec file to use
    #[clap(long)]
    pub chain_spec_file: Option<PathBuf>,

    /// Sentry GRPC service URL
    #[clap(
        long = "sentry.api.addr",
        help = "Sentry GRPC service URLs as 'http://host:port'"
    )]
    pub sentry_api_addr: Option<String>,

    #[clap(flatten)]
    pub sentry_opts: akula::sentry::Opts,

    /// Last block where to sync to.
    #[clap(long)]
    pub max_block: Option<BlockNumber>,

    /// Start with unwinding to this block.
    #[clap(long)]
    pub start_with_unwind: Option<BlockNumber>,

    /// Turn on pruning.
    #[clap(long)]
    pub prune: bool,

    /// Use incremental staged sync.
    #[clap(long)]
    pub increment: Option<u64>,

    /// Sender recovery batch size (blocks)
    #[clap(long, default_value = "500000")]
    pub sender_recovery_batch_size: u64,

    /// Execution batch size (Ggas).
    #[clap(long, default_value = "5000")]
    pub execution_batch_size: u64,

    /// Execution history batch size (Ggas).
    #[clap(long, default_value = "250")]
    pub execution_history_batch_size: u64,

    /// Exit execution stage after batch.
    #[clap(long)]
    pub execution_exit_after_batch: bool,

    /// Skip commitment (state root) verification.
    #[clap(long)]
    pub skip_commitment: bool,

    /// Exit Akula after sync is complete and there's no progress.
    #[clap(long)]
    pub exit_after_sync: bool,

    /// Delay applied at the terminating stage.
    #[clap(long, default_value = "0")]
    pub delay_after_sync: u64,

    /// Enable JSONRPC at this address.
    #[clap(long)]
    pub rpc_listen_address: Option<SocketAddr>,
}

#[allow(unreachable_code)]
fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::parse();
    fdlimit::raise_fd_limit();

    akula_tracing::build_subscriber(Component::Core).init();

    std::thread::Builder::new()
        .stack_size(128 * 1024 * 1024)
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_stack_size(128 * 1024 * 1024)
                .build()?;

            rt.block_on(async move {
                info!("Starting Akula ({})", version_string());

                let chain_config = if let Some(chain) = opt.chain {
                    let chain_config = ChainConfig::new(&chain)?;
                    Some(chain_config.chain_spec)
                } else if let Some(chain_path) = opt.chain_spec_file {
                    Some(ron::de::from_reader(File::open(chain_path)?)?)
                } else {
                    None
                };

                std::fs::create_dir_all(&opt.data_dir.0)?;
                let akula_chain_data_dir = opt.data_dir.chain_data_dir();
                let etl_temp_path = opt.data_dir.etl_temp_dir();
                let _ = std::fs::remove_dir_all(&etl_temp_path);
                std::fs::create_dir_all(&etl_temp_path)?;
                let etl_temp_dir = Arc::new(
                    tempfile::tempdir_in(&etl_temp_path)
                        .context("failed to create ETL temp dir")?,
                );
                let db = Arc::new(akula::kv::new_database(&akula_chain_data_dir)?);
                let chainspec = {
                    let span = span!(Level::INFO, "", " Genesis initialization ");
                    let _g = span.enter();
                    let txn = db.begin_mutable()?;
                    let (chainspec, initialized) =
                        akula::genesis::initialize_genesis(&txn, &*etl_temp_dir, chain_config)?;
                    if initialized {
                        txn.commit()?;
                    }

                    chainspec
                };

                let consensus: Arc<dyn Consensus> = engine_factory(chainspec.clone())?.into();

                let chain_config = ChainConfig::from(chainspec);

                if let Some(listen_address) = opt.rpc_listen_address {
                    let db = db.clone();
                    tokio::spawn(async move {
                        let server = HttpServerBuilder::default()
                            .build(listen_address)
                            .await
                            .unwrap();

                        let mut api = Methods::new();
                        api.merge(
                            EthApiServerImpl {
                                db: db.clone(),
                                call_gas_limit: 100_000_000,
                            }
                            .into_rpc(),
                        )
                        .unwrap();
                        api.merge(NetApiServerImpl.into_rpc()).unwrap();
                        api.merge(ErigonApiServerImpl { db: db.clone() }.into_rpc())
                            .unwrap();
                        api.merge(OtterscanApiServerImpl { db }.into_rpc()).unwrap();

                        let _server_handle = server.start(api).unwrap();

                        pending::<()>().await
                    });
                }

                // staged sync setup
                let mut staged_sync = stagedsync::StagedSync::new();
                staged_sync.set_min_progress_to_commit_after_stage(if opt.prune {
                    u64::MAX
                } else {
                    1024
                });
                if opt.prune {
                    staged_sync.set_pruning_interval(90_000);
                }
                staged_sync.set_max_block(opt.max_block);
                staged_sync.start_with_unwind(opt.start_with_unwind);
                staged_sync.set_exit_after_sync(opt.exit_after_sync);

                if opt.delay_after_sync > 0 {
                    staged_sync
                        .set_delay_after_sync(Some(Duration::from_millis(opt.delay_after_sync)));
                }

                let sentries = if let Some(raw_str) = opt.sentry_api_addr {
                    raw_str
                        .split(',')
                        .filter_map(|s| s.parse::<Uri>().ok())
                        .collect::<Vec<_>>()
                } else {
                    let max_peers = opt.sentry_opts.max_peers;
                    let sentry_api_addr = opt.sentry_opts.sentry_addr;
                    let swarm = akula::sentry::run(opt.sentry_opts, chain_config.dns()).await?;

                    let current_stage = staged_sync.current_stage();

                    tokio::spawn(async move {
                        loop {
                            if let Some(stage) = *current_stage.borrow() {
                                if stage == HEADERS || stage == BODIES {
                                    info!(
                                        "P2P node peer info: {} active (+{} dialing) / {} max.",
                                        swarm.connected_peers(),
                                        swarm.dialing(),
                                        max_peers
                                    );
                                }
                            }

                            sleep(Duration::from_secs(5)).await;
                        }
                    });

                    vec![format!("http://{sentry_api_addr}").parse()?]
                };

                let mut builder = NodeBuilder::default()
                    .set_stash(db.clone())
                    .set_config(chain_config.clone());
                for sentry_api_addr in sentries {
                    builder = builder.add_sentry(sentry_api_addr);
                }

                let node = Arc::new(builder.build()?);
                tokio::spawn({
                    let node = node.clone();
                    async move {
                        node.start_sync().await.unwrap();
                    }
                });

                staged_sync.push(
                    HeaderDownload {
                        node: node.clone(),
                        consensus: consensus.clone(),
                        requests: Default::default(),
                        max_block: opt.max_block.unwrap_or_else(|| u64::MAX.into()),
                        graph: Default::default(),
                    },
                    false,
                );
                staged_sync.push(TotalGasIndex, false);
                staged_sync.push(
                    BlockHashes {
                        temp_dir: etl_temp_dir.clone(),
                    },
                    false,
                );
                staged_sync.push(BodyDownload { node, consensus }, false);
                staged_sync.push(TotalTxIndex, false);
                staged_sync.push(
                    SenderRecovery {
                        batch_size: opt.sender_recovery_batch_size.try_into().unwrap(),
                    },
                    false,
                );
                staged_sync.push(
                    Execution {
                        batch_size: opt.execution_batch_size.saturating_mul(1_000_000_000_u64),
                        history_batch_size: opt
                            .execution_history_batch_size
                            .saturating_mul(1_000_000_000_u64),
                        exit_after_batch: opt.execution_exit_after_batch,
                        batch_until: None,
                        commit_every: None,
                    },
                    false,
                );
                if !opt.skip_commitment {
                    staged_sync.push(HashState::new(etl_temp_dir.clone(), None), !opt.prune);
                    staged_sync.push_with_unwind_priority(
                        Interhashes::new(etl_temp_dir.clone(), None),
                        !opt.prune,
                        1,
                    );
                }
                staged_sync.push(
                    AccountHistoryIndex {
                        temp_dir: etl_temp_dir.clone(),
                        flush_interval: 50_000,
                    },
                    !opt.prune,
                );
                staged_sync.push(
                    StorageHistoryIndex {
                        temp_dir: etl_temp_dir.clone(),
                        flush_interval: 50_000,
                    },
                    !opt.prune,
                );
                staged_sync.push(
                    TxLookup {
                        temp_dir: etl_temp_dir.clone(),
                    },
                    !opt.prune,
                );
                staged_sync.push(
                    CallTraceIndex {
                        temp_dir: etl_temp_dir.clone(),
                        flush_interval: 50_000,
                    },
                    !opt.prune,
                );
                staged_sync.push(Finish, !opt.prune);

                info!("Running staged sync");
                staged_sync.run(&db).await?;

                if opt.exit_after_sync {
                    Ok(())
                } else {
                    pending().await
                }
            })
        })?
        .join()
        .unwrap_or_else(|e| panic::resume_unwind(e))
}
