use akula::{
    binutil::AkulaDataDir,
    downloader::sentry_status_provider::SentryStatusProvider,
    kv::{
        tables::{self, ErasedTable},
        traits::*,
    },
    models::*,
    sentry::{
        sentry_client_connector::SentryClientConnectorImpl,
        sentry_client_reactor::SentryClientReactor,
    },
    stagedsync::{self, stage::*, stages::FINISH},
    stages::*,
    version_string, StageId,
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use clap::Parser;
use rayon::prelude::*;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::pin;
use tokio_stream::StreamExt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Akula", about = "Next-generation Ethereum implementation.")]
pub struct Opt {
    /// Path to Erigon database directory, where to get blocks from.
    #[clap(long = "erigon-datadir", parse(from_os_str))]
    pub erigon_data_dir: Option<PathBuf>,

    /// Path to Akula database directory.
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,

    /// Name of the testnet to join
    #[clap(
        long = "chain",
        help = "Name of the testnet to join",
        default_value = "mainnet"
    )]
    pub chain_name: String,

    /// Sentry GRPC service URL
    #[clap(
        long = "sentry.api.addr",
        help = "Sentry GRPC service URL as 'http://host:port'",
        default_value = "http://localhost:8000"
    )]
    pub sentry_api_addr: akula::sentry::sentry_address::SentryAddress,

    /// Last block where to sync to.
    #[clap(long)]
    pub max_block: Option<BlockNumber>,

    /// Downloader options.
    #[clap(flatten)]
    pub downloader_opts: akula::downloader::opts::Opts,

    /// Sender recovery batch size (blocks)
    #[clap(long, default_value = "50000")]
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

    /// Exit Akula after sync is complete and there's no progress.
    #[clap(long)]
    pub exit_after_sync: bool,

    /// Delay applied at the terminating stage.
    #[clap(long, default_value = "2000")]
    pub delay_after_sync: u64,
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

        let mut erigon_canonical_cur = erigon_tx.cursor(tables::CanonicalHeader).await?;
        let mut canonical_cur = tx.mutable_cursor(tables::CanonicalHeader).await?;
        let mut erigon_header_cur = erigon_tx.cursor(tables::Header.erased()).await?;
        let mut header_cur = tx.mutable_cursor(tables::Header).await?;
        let mut erigon_td_cur = erigon_tx.cursor(tables::HeadersTotalDifficulty).await?;
        let mut td_cur = tx.mutable_cursor(tables::HeadersTotalDifficulty).await?;

        if erigon_tx
            .get(tables::CanonicalHeader, highest_block)
            .await?
            != tx.get(tables::CanonicalHeader, highest_block).await?
        {
            return Ok(ExecOutput::Unwind {
                unwind_to: BlockNumber(highest_block.0 - 1),
            });
        }

        let walker = walk(&mut erigon_canonical_cur, Some(highest_block + 1));
        pin!(walker);
        while let Some((block_number, canonical_hash)) = walker.try_next().await? {
            if block_number > self.max_block.unwrap_or(BlockNumber(u64::MAX)) {
                break;
            }

            highest_block = block_number;

            canonical_cur.append(block_number, canonical_hash).await?;
            header_cur
                .append(
                    (block_number, canonical_hash),
                    rlp::decode(
                        &erigon_header_cur
                            .seek_exact(
                                TableEncode::encode((block_number, canonical_hash)).to_vec(),
                            )
                            .await?
                            .unwrap()
                            .1,
                    )?,
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
        })
    }

    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut canonical_cur = tx.mutable_cursor(tables::CanonicalHeader).await?;

        while let Some((block_num, _)) = canonical_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            canonical_cur.delete_current().await?;
        }

        let mut header_cur = tx.mutable_cursor(tables::Header).await?;
        while let Some(((block_num, _), _)) = header_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            header_cur.delete_current().await?;
        }

        let mut td_cur = tx.mutable_cursor(tables::HeadersTotalDifficulty).await?;
        while let Some(((block_num, _), _)) = td_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            td_cur.delete_current().await?;
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[derive(Debug)]
struct ConvertBodies<Source>
where
    Source: KV,
{
    db: Arc<Source>,
    commit_after: Duration,
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
        const BUFFERING_FACTOR: usize = 100_000;
        let erigon_tx = self.db.begin().await?;

        if erigon_tx
            .get(tables::CanonicalHeader, highest_block)
            .await?
            != tx.get(tables::CanonicalHeader, highest_block).await?
        {
            return Ok(ExecOutput::Unwind {
                unwind_to: BlockNumber(highest_block.0 - 1),
            });
        }

        let mut canonical_header_cur = tx.cursor(tables::CanonicalHeader).await?;

        let mut erigon_body_cur = erigon_tx.cursor(tables::BlockBody.erased()).await?;
        let mut body_cur = tx.mutable_cursor(tables::BlockBody).await?;

        let mut erigon_tx_cur = erigon_tx.cursor(tables::BlockTransaction.erased()).await?;
        let mut tx_cur = tx.mutable_cursor(tables::BlockTransaction.erased()).await?;

        let prev_body = tx
            .get(
                tables::BlockBody,
                (
                    highest_block,
                    tx.get(tables::CanonicalHeader, highest_block)
                        .await?
                        .unwrap(),
                ),
            )
            .await?
            .unwrap();

        let mut starting_index = prev_body.base_tx_id + prev_body.tx_amount as u64;
        let canonical_header_walker = walk(&mut canonical_header_cur, Some(highest_block + 1));
        pin!(canonical_header_walker);
        let erigon_body_walker = walk(
            &mut erigon_body_cur,
            Some(TableEncode::encode(highest_block + 1).to_vec()),
        );
        pin!(erigon_body_walker);
        let mut batch = Vec::with_capacity(BUFFERING_FACTOR);
        let mut converted = Vec::new();

        let mut extracted_blocks_num = 0;
        let mut extracted_txs_num = 0;
        let started_at = Instant::now();
        let mut last_check = started_at;

        let done = loop {
            let mut no_more_bodies = true;
            let mut accum_txs = 0;
            'l: while let Some((block_num, block_hash)) = canonical_header_walker.try_next().await?
            {
                loop {
                    if let Some((k, v)) = erigon_body_walker.try_next().await? {
                        let (body_block_num, body_block_hash) = <(BlockNumber, H256)>::decode(&k)?;
                        if body_block_num > block_num {
                            break 'l;
                        }

                        if body_block_hash != block_hash {
                            continue;
                        }

                        let body = rlp::decode::<BodyForStorage>(&v)?;

                        let base_tx_id = body.base_tx_id;

                        let tx_amount = usize::try_from(body.tx_amount)?;
                        let txs = walk(&mut erigon_tx_cur, Some(base_tx_id.encode().to_vec()))
                            .map(|res| res.map(|(_, tx)| tx))
                            .take(tx_amount)
                            .collect::<anyhow::Result<Vec<_>>>()
                            .await?;

                        if txs.len() != tx_amount {
                            bail!(
                                "Invalid tx amount in Erigon for block #{}/{}: {} != {}",
                                block_num,
                                block_hash,
                                tx_amount,
                                txs.len()
                            );
                        }

                        accum_txs += tx_amount;
                        batch.push((block_num, block_hash, body, txs));

                        break;
                    } else {
                        break 'l;
                    }
                }

                if accum_txs > MAX_TXS_PER_BATCH {
                    no_more_bodies = false;
                    break;
                }
            }

            debug!(
                "Read a batch of {} blocks with {} transactions",
                batch.len(),
                accum_txs
            );

            extracted_blocks_num += batch.len();
            extracted_txs_num += accum_txs;

            converted.reserve(batch.len());
            batch
                .par_drain(..)
                .map(move |(block_number, block_hash, body, txs)| {
                    Ok::<_, anyhow::Error>((
                        block_number,
                        block_hash,
                        body.uncles,
                        txs.into_iter()
                            .map(|v| {
                                Ok(rlp::decode::<akula::models::MessageWithSignature>(&v)?
                                    .encode()
                                    .to_vec())
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
                    tx_amount: txs.len().try_into()?,
                    uncles,
                };

                body_cur.append((block_num, block_hash), body).await?;

                for tx in txs {
                    tx_cur
                        .append(
                            ErasedTable::<tables::BlockTransaction>::encode_key(starting_index)
                                .to_vec(),
                            tx,
                        )
                        .await?;
                    starting_index.0 += 1;
                }
            }

            if no_more_bodies {
                break true;
            }

            let now = Instant::now();
            let elapsed = now - last_check;
            if elapsed > Duration::from_secs(30) {
                info!(
                    "Highest block {}, batch size: {} blocks with {} transactions, {} tx/sec",
                    highest_block.0,
                    extracted_blocks_num,
                    extracted_txs_num,
                    extracted_txs_num as f64
                        / (elapsed.as_secs() as f64 + (elapsed.subsec_millis() as f64 / 1000_f64))
                );

                if now - started_at > self.commit_after {
                    break false;
                }

                extracted_blocks_num = 0;
                extracted_txs_num = 0;
                last_check = Instant::now();
            }
        };

        Ok(ExecOutput::Progress {
            stage_progress: highest_block,
            done,
        })
    }
    async fn unwind<'tx>(
        &self,
        tx: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        let mut block_body_cur = tx.mutable_cursor(tables::BlockBody).await?;
        let mut block_tx_cur = tx.mutable_cursor(tables::BlockTransaction).await?;
        while let Some(((block_num, _), body)) = block_body_cur.last().await? {
            if block_num <= input.unwind_to {
                break;
            }

            block_body_cur.delete_current().await?;

            let mut deleted = 0;
            while deleted < body.tx_amount {
                let to_delete = body.base_tx_id + deleted;
                if block_tx_cur.seek(to_delete).await?.is_some() {
                    block_tx_cur.delete_current().await?;
                }

                deleted += 1;
            }
        }

        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[derive(Debug)]
struct TerminatingStage {
    max_block: Option<BlockNumber>,
    exit_after_sync: bool,
    delay_after_sync: Duration,
}

#[async_trait]
impl<'db, RwTx> Stage<'db, RwTx> for TerminatingStage
where
    RwTx: MutableTransaction<'db>,
{
    fn id(&self) -> StageId {
        FINISH
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
                }
            } else {
                if self.exit_after_sync {
                    info!("Sync complete, exiting.");
                    std::process::exit(0)
                }

                tokio::time::sleep(self.delay_after_sync).await;

                ExecOutput::Progress {
                    stage_progress: prev_stage,
                    done: true,
                }
            },
        )
    }
    async fn unwind<'tx>(
        &self,
        _: &'tx mut RwTx,
        input: UnwindInput,
    ) -> anyhow::Result<UnwindOutput>
    where
        'db: 'tx,
    {
        Ok(UnwindOutput {
            stage_progress: input.unwind_to,
        })
    }
}

#[allow(unreachable_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::parse();

    let nocolor = std::env::var("RUST_LOG_STYLE")
        .map(|val| val == "never")
        .unwrap_or(false);

    // tracing setup
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(!nocolor),
        )
        .with(env_filter)
        .init();

    info!("Starting Akula ({})", version_string());

    let chains_config = akula::sentry::chain_config::ChainsConfig::new()?;
    let chain_config = chains_config.get(&opt.chain_name)?;

    // database setup
    let erigon_db = if let Some(erigon_data_dir) = opt.erigon_data_dir {
        let erigon_chain_data_dir = erigon_data_dir.join("chaindata");
        let erigon_db = akula::kv::mdbx::Environment::<mdbx::NoWriteMap>::open_ro(
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
    let etl_temp_path = opt.data_dir.etl_temp_dir();
    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let etl_temp_dir =
        Arc::new(tempfile::tempdir_in(&etl_temp_path).context("failed to create ETL temp dir")?);
    let db = akula::kv::new_database(&akula_chain_data_dir)?;
    async {
        let txn = db.begin_mutable().await?;
        if akula::genesis::initialize_genesis(
            &txn,
            &*etl_temp_dir,
            chain_config.chain_spec().clone(),
        )
        .await?
        {
            txn.commit().await?;
        }

        Ok::<_, anyhow::Error>(())
    }
    .instrument(span!(Level::INFO, "", " Genesis initialization "))
    .await?;

    let sentry_status_provider = SentryStatusProvider::new(chain_config.clone());
    // staged sync setup
    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.set_min_progress_to_commit_after_stage(1024);
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
    staged_sync.push(BlockHashes {
        temp_dir: etl_temp_dir.clone(),
    });
    if let Some(erigon_db) = erigon_db {
        staged_sync.push(ConvertBodies {
            db: erigon_db,
            commit_after: Duration::from_secs(120),
        });
    } else {
        // also add body download stage here
    }
    staged_sync.push(CumulativeIndex);
    staged_sync.push(SenderRecovery {
        batch_size: opt.sender_recovery_batch_size.try_into().unwrap(),
    });
    staged_sync.push(Execution {
        batch_size: opt.execution_batch_size.saturating_mul(1_000_000_000_u64),
        history_batch_size: opt
            .execution_history_batch_size
            .saturating_mul(1_000_000_000_u64),
        exit_after_batch: opt.execution_exit_after_batch,
        batch_until: None,
        commit_every: None,
        prune_from: BlockNumber(0),
    });
    staged_sync.push(HashState::new(etl_temp_dir.clone(), None));
    staged_sync.push(Interhashes::new(etl_temp_dir.clone(), None));
    staged_sync.push(CallTraceIndex {
        temp_dir: etl_temp_dir.clone(),
        flush_interval: 50_000,
    });
    staged_sync.push(TerminatingStage {
        max_block: opt.max_block,
        exit_after_sync: opt.exit_after_sync,
        delay_after_sync: Duration::from_millis(opt.delay_after_sync),
    });

    info!("Running staged sync");
    staged_sync.run(&db).await?;

    Ok(())
}
