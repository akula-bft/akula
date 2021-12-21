use akula::{
    binutil::AkulaDataDir,
    hex_to_bytes,
    kv::{
        tables::{self, CHAINDATA_TABLES},
        traits::*,
    },
    models::*,
    stagedsync::{self},
    stages::*,
};
use anyhow::{bail, ensure, format_err, Context};
use bytes::Bytes;
use itertools::Itertools;
use std::{borrow::Cow, path::PathBuf};
use structopt::StructOpt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Akula Toolbox", about = "Utilities for Akula Ethereum client")]
struct Opt {
    #[structopt(long = "datadir", help = "Database directory path", default_value)]
    pub data_dir: AkulaDataDir,

    #[structopt(subcommand)]
    pub command: OptCommand,
}

#[derive(StructOpt)]
pub enum OptCommand {
    /// Print database statistics
    DbStats {
        /// Whether to print CSV
        #[structopt(long)]
        csv: bool,
    },

    /// Query database
    DbQuery {
        #[structopt(long)]
        table: String,
        #[structopt(long, parse(try_from_str = hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[structopt(long)]
        table: String,
        #[structopt(long, parse(try_from_str = hex_to_bytes))]
        starting_key: Option<Bytes>,
        #[structopt(long)]
        max_entries: Option<usize>,
    },

    /// Check table equality in two databases
    CheckEqual {
        #[structopt(long, parse(from_os_str))]
        db1: PathBuf,
        #[structopt(long, parse(from_os_str))]
        db2: PathBuf,
        #[structopt(long)]
        table: String,
    },

    /// Execute Block Hashes stage
    Blockhashes,

    /// Execute HeaderDownload stage
    #[structopt(name = "download-headers", about = "Run block headers downloader")]
    HeaderDownload {
        #[structopt(flatten)]
        opts: HeaderDownloadOpts,
    },

    ReadBlock {
        block_number: BlockNumber,
    },
}

#[derive(StructOpt)]
pub struct HeaderDownloadOpts {
    #[structopt(
        long = "chain",
        help = "Name of the testnet to join",
        default_value = "mainnet"
    )]
    pub chain_name: String,

    #[structopt(
        long = "sentry.api.addr",
        help = "Sentry GRPC service URL as 'http://host:port'",
        default_value = "http://localhost:8000"
    )]
    pub sentry_api_addr: akula::sentry::sentry_address::SentryAddress,

    #[structopt(flatten)]
    pub downloader_opts: akula::downloader::opts::Opts,
}

async fn blockhashes(data_dir: AkulaDataDir) -> anyhow::Result<()> {
    std::fs::create_dir_all(&data_dir.0)?;
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(BlockHashes);
    staged_sync.run(&env).await?;
}

#[allow(unreachable_code)]
async fn header_download(data_dir: AkulaDataDir, opts: HeaderDownloadOpts) -> anyhow::Result<()> {
    let chains_config = akula::sentry::chain_config::ChainsConfig::new()?;
    let chain_config = chains_config.get(&opts.chain_name)?;

    let sentry_api_addr = opts.sentry_api_addr.clone();
    let sentry_connector =
        akula::sentry::sentry_client_connector::SentryClientConnectorImpl::new(sentry_api_addr);

    let sentry_status_provider =
        akula::downloader::sentry_status_provider::SentryStatusProvider::new(chain_config.clone());
    let mut sentry_reactor = akula::sentry::sentry_client_reactor::SentryClientReactor::new(
        Box::new(sentry_connector),
        sentry_status_provider.current_status_stream(),
    );
    sentry_reactor.start()?;
    let sentry = sentry_reactor.into_shared();

    let stage = akula::stages::HeaderDownload::new(
        chain_config,
        opts.downloader_opts.headers_mem_limit(),
        opts.downloader_opts.headers_batch_size,
        sentry.clone(),
        sentry_status_provider,
    )?;

    std::fs::create_dir_all(&data_dir.0)?;
    let db = akula::kv::new_database(&data_dir.chain_data_dir())?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(stage);
    staged_sync.run(&db).await?;

    sentry.write().await.stop().await
}

async fn table_sizes(data_dir: AkulaDataDir, csv: bool) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        Default::default(),
    )?;
    let mut sizes = env
        .begin()
        .await?
        .table_sizes()?
        .into_iter()
        .collect::<Vec<_>>();
    sizes.sort_by_key(|(_, size)| *size);

    let mut out = Vec::new();
    if csv {
        out.push("Table,Size".to_string());
        for (table, size) in &sizes {
            out.push(format!("{},{}", table, size));
        }
    } else {
        for (table, size) in &sizes {
            out.push(format!("{} - {}", table, bytesize::ByteSize::b(*size)));
        }
        out.push(format!(
            "TOTAL: {}",
            bytesize::ByteSize::b(sizes.into_iter().map(|(_, size)| size).sum())
        ));
    }

    for line in out {
        println!("{}", line);
    }
    Ok(())
}

async fn db_query(data_dir: AkulaDataDir, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        Default::default(),
    )?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let value = txn.get::<Vec<u8>>(&db, &key)?;

    println!("{:?}", value.as_ref().map(hex::encode));

    if let Some(v) = value {
        println!(
            "{:?}",
            rlp::decode::<akula::models::MessageWithSignature>(&v)?.hash()
        );
    }

    Ok(())
}

async fn db_walk(
    data_dir: AkulaDataDir,
    table: String,
    starting_key: Option<Bytes>,
    max_entries: Option<usize>,
) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        Default::default(),
    )?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur = txn.cursor(&db)?;
    for (i, item) in if let Some(starting_key) = starting_key {
        cur.iter_from::<Cow<[u8]>, Cow<[u8]>>(&starting_key)
    } else {
        cur.iter::<Cow<[u8]>, Cow<[u8]>>()
    }
    .enumerate()
    .take(max_entries.unwrap_or(usize::MAX))
    {
        let (k, v) = item?;
        println!(
            "{} / {:?} / {:?} / {:?} / {:?}",
            i,
            hex::encode(k),
            hex::encode(&v),
            Account::decode_for_storage(&v),
            BlockHeader::decode(&v)
        );
    }

    Ok(())
}

async fn check_table_eq(db1_path: PathBuf, db2_path: PathBuf, table: String) -> anyhow::Result<()> {
    let env1 = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db1_path,
        Default::default(),
    )?;
    let env2 = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db2_path,
        Default::default(),
    )?;

    let txn1 = env1.begin_ro_txn()?;
    let txn2 = env2.begin_ro_txn()?;
    let db1 = txn1
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let db2 = txn2
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let mut cur1 = txn1.cursor(&db1)?;
    let mut cur2 = txn2.cursor(&db2)?;

    let mut i = 0;
    let mut excess = 0;
    for res in cur1
        .iter_start::<Cow<[u8]>, Cow<[u8]>>()
        .zip_longest(cur2.iter_start::<Cow<[u8]>, Cow<[u8]>>())
    {
        if i > 0 && i % 1_000_000 == 0 {
            info!("Checked {} entries", i);
        }
        match res {
            itertools::EitherOrBoth::Both(a, b) => {
                let (k1, v1) = a?;
                let (k2, v2) = b?;
                ensure!(
                    k1 == k2 && v1 == v2,
                    "MISMATCH DETECTED: {}: {} != {}: {}\n{:?}\n{:?}",
                    hex::encode(&k1),
                    hex::encode(&v1),
                    hex::encode(&k2),
                    hex::encode(&v2),
                    Account::decode_for_storage(&v1),
                    Account::decode_for_storage(&v2),
                );
            }
            itertools::EitherOrBoth::Left(_) => excess -= 1,
            itertools::EitherOrBoth::Right(_) => excess += 1,
        }

        i += 1;
    }

    match excess.cmp(&0) {
        std::cmp::Ordering::Less => {
            bail!("db1 longer than db2 by {} entries", -excess);
        }
        std::cmp::Ordering::Equal => {}
        std::cmp::Ordering::Greater => {
            bail!("db2 longer than db1 by {} entries", excess);
        }
    }

    info!("Check complete. {} entries scanned.", i);

    Ok(())
}

async fn read_block(data_dir: AkulaDataDir, block_num: BlockNumber) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        CHAINDATA_TABLES.clone(),
    )?;

    let tx = env.begin().await?;

    let canonical_hash = tx
        .get(&tables::CanonicalHeader, block_num)
        .await?
        .ok_or_else(|| format_err!("no such canonical block"))?;
    let header = tx
        .get(&tables::Header, (block_num, canonical_hash))
        .await?
        .ok_or_else(|| format_err!("header not found"))?;
    let body =
        akula::accessors::chain::block_body::read_without_senders(&tx, canonical_hash, block_num)
            .await?
            .ok_or_else(|| format_err!("block body not found"))?;

    let partial_header = PartialHeader::from(header.clone());

    let block = Block::new(partial_header.clone(), body.transactions, body.ommers);

    ensure!(
        block.header.transactions_root == header.transactions_root,
        "root mismatch: expected in header {:?}, computed {:?}",
        header.transactions_root,
        block.header.transactions_root
    );
    ensure!(
        block.header.ommers_hash == header.ommers_hash,
        "root mismatch: expected in header {:?}, computed {:?}",
        header.ommers_hash,
        block.header.ommers_hash
    );

    println!("{:?}", partial_header);
    println!("OMMERS:");
    for (i, v) in block.ommers.into_iter().enumerate() {
        println!("[{}] {:?}", i, v);
    }

    println!("TRANSACTIONS:");
    for (i, v) in block.transactions.into_iter().enumerate() {
        println!("[{}/{:?}] {:?}", i, v.hash(), v);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::from_args();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter)
        .init();

    match opt.command {
        OptCommand::DbStats { csv } => table_sizes(opt.data_dir, csv).await?,
        OptCommand::Blockhashes => blockhashes(opt.data_dir).await?,
        OptCommand::DbQuery { table, key } => db_query(opt.data_dir, table, key).await?,
        OptCommand::DbWalk {
            table,
            starting_key,
            max_entries,
        } => db_walk(opt.data_dir, table, starting_key, max_entries).await?,
        OptCommand::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table).await?,
        OptCommand::HeaderDownload { opts } => header_download(opt.data_dir, opts).await?,
        OptCommand::ReadBlock { block_number } => read_block(opt.data_dir, block_number).await?,
    }

    Ok(())
}
