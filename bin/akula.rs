use akula::{
    binutil::AkulaDataDir,
    downloader::sentry_status_provider::SentryStatusProvider,
    kv::{
        tables,
        traits::{MutableKV, KV},
        TableEncode,
    },
    models::*,
    sentry::{
        sentry_client_connector::SentryClientConnectorImpl,
        sentry_client_reactor::SentryClientReactor,
    },
    stagedsync::{
        self,
        stage::{ExecOutput, Stage, StageInput, UnwindInput},
    },
    stages::*,
    version_string, Cursor, MutableCursor, MutableTransaction, StageId, Transaction,
};
use anyhow::bail;
use async_trait::async_trait;
use rayon::prelude::*;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Akula", about = "Next-generation Ethereum implementation.")]
pub struct Opt {
    /// Path to Erigon database directory, where to get blocks from.
    #[structopt(long = "erigon-datadir", parse(from_os_str))]
    pub erigon_data_dir: Option<PathBuf>,

    /// Path to Akula database directory.
    #[structopt(long = "datadir", help = "Database directory path", default_value)]
    pub data_dir: AkulaDataDir,

    /// Name of the testnet to join
    #[structopt(
        long = "chain",
        help = "Name of the testnet to join",
        default_value = "mainnet"
    )]
    pub chain_name: String,

    /// Sentry GRPC service URL
    #[structopt(
        long = "sentry.api.addr",
        help = "Sentry GRPC service URL as 'http://host:port'",
        default_value = "http://localhost:8000"
    )]
    pub sentry_api_addr: akula::sentry::sentry_address::SentryAddress,

    /// Last block where to sync to.
    #[structopt(long)]
    pub max_block: Option<BlockNumber>,

    /// Downloader options.
    #[structopt(flatten)]
    pub downloader_opts: akula::downloader::opts::Opts,

    /// Execution batch size (Ggas).
    #[structopt(long, default_value = "1000")]
    pub execution_batch_size: u64,

    /// Exit execution stage after batch.
    #[structopt(long, env)]
    pub execution_exit_after_batch: bool,

    /// Enables tokio console server
    #[structopt(long, env)]
    pub tokio_console: bool,
}

#[derive(Debug)]
struct ConvertHeaders<Source>
where
    Source: KV,
{
    db: Arc<Source>,
    max_block: Option<BlockNumber>,
}

#[async_trait]
impl<'db, RwTx, Source> Stage<'db, RwTx> for ConvertHeaders<Source>
where
    Source: KV,
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("ConvertHeaders")
    }
    fn description(&self) -> &'static str {
        ""
    }
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        let erigon_tx = self.db.begin().await?;

        let mut erigon_canonical_cur = erigon_tx.cursor(&tables::CanonicalHeader).await?;
        let mut canonical_cur = tx.mutable_cursor(&tables::CanonicalHeader).await?;
        let mut erigon_header_cur = erigon_tx.cursor(&tables::Header).await?;
        let mut header_cur = tx.mutable_cursor(&tables::Header).await?;
        let mut erigon_td_cur = erigon_tx.cursor(&tables::HeadersTotalDifficulty).await?;
        let mut td_cur = tx.mutable_cursor(&tables::HeadersTotalDifficulty).await?;

        assert_eq!(
            erigon_tx
                .get(&tables::CanonicalHeader, highest_block)
                .await?,
            tx.get(&tables::CanonicalHeader, highest_block).await?
        );

        let mut walker = erigon_canonical_cur.walk(Some(highest_block + 1));
        while let Some((block_number, canonical_hash)) = walker.try_next().await? {
            if block_number > self.max_block.unwrap_or(BlockNumber(u64::MAX)) {
                break;
            }

            highest_block = block_number;

            canonical_cur.append(block_number, canonical_hash).await?;
            header_cur
                .append(
                    (block_number, canonical_hash),
                    erigon_header_cur
                        .seek_exact((block_number, canonical_hash))
                        .await?
                        .unwrap()
                        .1,
                )
                .await?;
            td_cur
                .append(
                    (block_number, canonical_hash),
                    erigon_td_cur
                        .seek_exact((block_number, canonical_hash))
                        .await?
                        .unwrap()
                        .1,
                )
                .await?;

            if block_number.0 % 500_000 == 0 {
                info!("Extracted block {}", block_number);
            }
        }

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done: true,
            must_commit: highest_block > original_highest_block,
        })
    }

    async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        todo!()
    }
}

#[derive(Debug)]
struct ConvertBodies<Source>
where
    Source: KV,
{
    db: Arc<Source>,
}

#[async_trait]
impl<'db, RwTx, Source> Stage<'db, RwTx> for ConvertBodies<Source>
where
    Source: KV,
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("ConvertBodies")
    }
    fn description(&self) -> &'static str {
        ""
    }
    #[allow(clippy::redundant_closure_call)]
    async fn execute<'tx>(&self, tx: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let original_highest_block = input.stage_progress.unwrap_or(BlockNumber(0));
        let mut highest_block = original_highest_block;

        const MAX_TXS_PER_BATCH: usize = 500_000;
        const BUFFERING_FACTOR: usize = 500_000;
        let erigon_tx = self.db.begin().await?;
        let mut canonical_header_cur = tx.cursor(&tables::CanonicalHeader).await?;

        let mut erigon_body_cur = erigon_tx.cursor(&tables::BlockBody).await?;
        let mut body_cur = tx.mutable_cursor(&tables::BlockBody).await?;

        let mut erigon_tx_cur = erigon_tx.cursor(&tables::BlockTransaction.erased()).await?;
        let mut tx_cur = tx
            .mutable_cursor(&tables::BlockTransaction.erased())
            .await?;

        let prev_body = tx
            .get(
                &tables::BlockBody,
                (
                    highest_block,
                    tx.get(&tables::CanonicalHeader, highest_block)
                        .await?
                        .unwrap(),
                ),
            )
            .await?
            .unwrap();

        let mut starting_index = prev_body.base_tx_id + prev_body.tx_amount as u64;
        let mut walker = canonical_header_cur.walk(Some(highest_block + 1));
        let mut batch = Vec::with_capacity(BUFFERING_FACTOR);
        let mut converted = Vec::with_capacity(BUFFERING_FACTOR);

        let mut extracted_blocks_num = 0;
        let mut extracted_txs_num = 0;

        let started_at = Instant::now();
        let done = loop {
            let mut no_more_bodies = false;
            let mut accum_txs = 0;
            while let Some((block_num, block_hash)) = walker.try_next().await? {
                if let Some((_, body)) = erigon_body_cur.seek_exact((block_num, block_hash)).await?
                {
                    let base_tx_id = body.base_tx_id;
                    let block_tx_base_key = base_tx_id.encode();

                    let txs = erigon_tx_cur
                        .walk(Some(block_tx_base_key.to_vec()))
                        .take(body.tx_amount)
                        .collect::<anyhow::Result<Vec<_>>>()
                        .await?;

                    if let Some((txkey, _)) = txs.first() {
                        if *txkey != block_tx_base_key {
                            bail!(
                                "Base txkey mismatch for block #{}/{}: {:?} != {:?}",
                                block_num,
                                block_hash,
                                txkey,
                                block_tx_base_key
                            );
                        }
                    }

                    if txs.len() != body.tx_amount {
                        bail!(
                            "Invalid tx amount in Erigon for block #{}/{}: {} != {}",
                            block_num,
                            block_hash,
                            body.tx_amount,
                            txs.len()
                        );
                    }

                    accum_txs += body.tx_amount;
                    batch.push((block_num, block_hash, body, txs));

                    if accum_txs > MAX_TXS_PER_BATCH {
                        break;
                    }
                } else {
                    no_more_bodies = true;
                    break;
                }
            }

            if batch.is_empty() {
                break true;
            }

            extracted_blocks_num += batch.len();
            extracted_txs_num += accum_txs;

            batch
                .par_drain(..)
                .map(move |(block_number, block_hash, body, txs)| {
                    Ok::<_, anyhow::Error>((
                        block_number,
                        block_hash,
                        body.uncles,
                        txs.into_iter()
                            .map(|(k, v)| {
                                Ok((
                                    k,
                                    rlp::decode::<akula::models::Transaction>(&v)?
                                        .encode()
                                        .to_vec(),
                                ))
                            })
                            .collect::<anyhow::Result<Vec<_>>>()?,
                    ))
                })
                .collect_into_vec(&mut converted);

            for res in converted.drain(..) {
                let (block_num, block_hash, uncles, txs) = res?;
                highest_block = block_num;
                let body = BodyForStorage {
                    base_tx_id: starting_index,
                    tx_amount: txs.len(),
                    uncles,
                };
                starting_index.0 += txs.len() as u64;

                body_cur.append((block_num, block_hash), body).await?;

                for (index, tx) in txs {
                    tx_cur.append(index, tx).await?;
                }
            }

            let now = Instant::now();
            let elapsed = now - started_at;
            if elapsed > Duration::from_secs(30) {
                info!(
                    "Highest block {}, batch size: {} blocks with {} transactions, {} tx/sec",
                    highest_block.0,
                    extracted_blocks_num,
                    extracted_txs_num,
                    extracted_txs_num as f64
                        / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                );

                break no_more_bodies;
            }
        };

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done,
            must_commit: highest_block > original_highest_block,
        })
    }
    async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        todo!()
    }
}

#[derive(Debug)]
struct TerminatingStage {
    max_block: Option<BlockNumber>,
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for TerminatingStage
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        StageId("TerminatingStage")
    }
    fn description(&self) -> &'static str {
        ""
    }
    async fn execute<'tx>(&self, _: &'tx mut RwTx, input: StageInput) -> anyhow::Result<ExecOutput>
    where
        'db: 'tx,
    {
        let prev_stage = input
            .previous_stage
            .map(|(_, b)| b)
            .unwrap_or(BlockNumber(0));
        let last_cycle_progress = input.stage_progress.unwrap_or(BlockNumber(0));
        Ok(
            if prev_stage > last_cycle_progress
                && prev_stage < self.max_block.unwrap_or(BlockNumber(u64::MAX))
            {
                ExecOutput::Progress {
                    stage_progress: prev_stage,
                    done: true,
                    must_commit: true,
                }
            } else {
                info!("Sync complete, exiting.");
                std::process::exit(0)
            },
        )
    }
    async fn unwind<'tx>(&self, _: &'tx mut RwTx, _: UnwindInput) -> anyhow::Result<()>
    where
        'db: 'tx,
    {
        Ok(())
    }
}

#[allow(unreachable_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::from_args();

    // tracing setup
    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    let registry = tracing_subscriber::registry()
        // the `TasksLayer` can be used in combination with other `tracing` layers...
        .with(tracing_subscriber::fmt::layer().with_target(false));

    if opt.tokio_console {
        let (layer, server) = console_subscriber::TasksLayer::new();
        registry
            .with(filter.add_directive("tokio=trace".parse()?))
            .with(layer)
            .init();
        tokio::spawn(async move { server.serve().await.expect("server failed") });
    } else {
        registry.with(filter).init();
    }

    info!("Starting Akula ({})", version_string());

    let chains_config = akula::sentry::chain_config::ChainsConfig::new()?;
    let chain_config = chains_config.get(&opt.chain_name)?;

    // database setup
    let erigon_db = if let Some(erigon_data_dir) = opt.erigon_data_dir {
        let erigon_chain_data_dir = erigon_data_dir.join("chaindata");
        let erigon_db = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
            mdbx::Environment::new(),
            &erigon_chain_data_dir,
            akula::kv::tables::CHAINDATA_TABLES.clone(),
        )?;
        Some(Arc::new(erigon_db))
    } else {
        None
    };

    std::fs::create_dir_all(&opt.data_dir.0)?;
    let akula_chain_data_dir = opt.data_dir.chain_data_dir();
    let db = akula::kv::new_database(&akula_chain_data_dir)?;
    async {
        let txn = db.begin_mutable().await?;
        if akula::genesis::initialize_genesis(&txn, chain_config.chain_spec().clone()).await? {
            txn.commit().await?;
        }

        Ok::<_, anyhow::Error>(())
    }
    .instrument(span!(Level::INFO, "", " Genesis initialization "))
    .await?;

    let sentry_status_provider = SentryStatusProvider::new(chain_config.clone());
    // staged sync setup
    let mut staged_sync = stagedsync::StagedSync::new();
    // staged_sync.set_min_progress_to_commit_after_stage(2);
    if let Some(erigon_db) = erigon_db.clone() {
        staged_sync.push(ConvertHeaders {
            db: erigon_db,
            max_block: opt.max_block,
        });
    } else {
        // sentry setup
        let mut sentry_reactor = SentryClientReactor::new(
            Box::new(SentryClientConnectorImpl::new(opt.sentry_api_addr.clone())),
            sentry_status_provider.current_status_stream(),
        );
        sentry_reactor.start()?;

        staged_sync.push(HeaderDownload::new(
            chain_config,
            opt.downloader_opts.headers_mem_limit(),
            opt.downloader_opts.headers_batch_size,
            sentry_reactor.into_shared(),
            sentry_status_provider,
        )?);
    }
    staged_sync.push(BlockHashes);
    if let Some(erigon_db) = erigon_db {
        staged_sync.push(ConvertBodies { db: erigon_db });
    } else {
        // also add body download stage here
    }
    staged_sync.push(CumulativeIndex);
    staged_sync.push(SenderRecovery);
    staged_sync.push(Execution {
        batch_size: opt.execution_batch_size.saturating_mul(1_000_000_000_u64),
        exit_after_batch: opt.execution_exit_after_batch,
        batch_until: None,
        commit_every: None,
        prune_from: BlockNumber(0),
    });
    staged_sync.push(HashState::new(None));
    staged_sync.push(Interhashes::new(None));
    staged_sync.push(TerminatingStage {
        max_block: opt.max_block,
    });

    info!("Running staged sync");
    staged_sync.run(&db).await?;

    Ok(())
}
