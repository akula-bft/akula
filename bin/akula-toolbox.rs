#![feature(never_type)]
use akula::{
    binutil::AkulaDataDir,
    consensus::engine_factory,
    hex_to_bytes,
    kv::{
        tables::{self, CHAINDATA_TABLES},
        traits::*,
    },
    models::*,
    p2p::peer::SentryClient,
    stagedsync,
    stages::*,
};
use anyhow::{bail, ensure, format_err, Context};
use bytes::Bytes;
use clap::Parser;
use itertools::Itertools;
use std::{borrow::Cow, collections::BTreeMap, path::PathBuf, sync::Arc};
use tokio::pin;
use tonic::transport::Channel;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Akula Toolbox", about = "Utilities for Akula Ethereum client")]
struct Opt {
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,

    #[clap(subcommand)]
    pub command: OptCommand,
}

#[derive(Parser)]
pub enum OptCommand {
    /// Print database statistics
    DbStats {
        /// Whether to print CSV
        #[clap(long)]
        csv: bool,
    },

    /// Query database
    DbQuery {
        #[clap(long)]
        table: String,
        #[clap(long, parse(try_from_str = hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[clap(long)]
        table: String,
        #[clap(long, parse(try_from_str = hex_to_bytes))]
        starting_key: Option<Bytes>,
        #[clap(long)]
        max_entries: Option<usize>,
    },

    /// Check table equality in two databases
    CheckEqual {
        #[clap(long, parse(from_os_str))]
        db1: PathBuf,
        #[clap(long, parse(from_os_str))]
        db2: PathBuf,
        #[clap(long)]
        table: String,
    },

    /// Execute Block Hashes stage
    Blockhashes,

    /// Execute HeaderDownload stage
    #[clap(name = "download-headers", about = "Run block headers downloader")]
    HeaderDownload {
        #[clap(
            long = "chain",
            help = "Name of the testnet to join",
            default_value = "mainnet"
        )]
        chain: String,

        #[clap(
            long = "sentry.api.addr",
            help = "Sentry GRPC service URL as 'http://host:port'",
            default_value = "http://localhost:8000"
        )]
        uri: tonic::transport::Uri,
    },

    ReadBlock {
        block_number: BlockNumber,
    },

    ReadAccount {
        address: Address,
        block_number: Option<BlockNumber>,
    },

    ReadAccountChanges {
        block: BlockNumber,
    },

    ReadStorage {
        address: Address,
    },

    ReadStorageChanges {
        block: BlockNumber,
    },
}

async fn download_headers(
    data_dir: AkulaDataDir,
    chain: String,
    uri: tonic::transport::Uri,
) -> anyhow::Result<()> {
    let chain_config = ChainConfig::new(chain.as_ref())?;
    let consensus = engine_factory(chain_config.chain_spec.clone())?.into();
    let conn = SentryClient::new(
        Channel::builder(uri)
            .http2_adaptive_window(true)
            .connect()
            .await?,
    );
    let chain_data_dir = data_dir.chain_data_dir();
    let etl_temp_path = data_dir.etl_temp_dir();

    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let env = akula::kv::new_database(&chain_data_dir)?;
    let txn = env.begin_mutable()?;
    akula::genesis::initialize_genesis(
        &txn,
        &*Arc::new(tempfile::tempdir_in(etl_temp_path).context("failed to create ETL temp dir")?),
        Some(chain_config.chain_spec.clone()),
    )?;

    txn.commit()?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(HeaderDownload::new(
        conn,
        consensus,
        chain_config,
        env.begin()?,
    )?);
    staged_sync.run(&env).await?;

    Ok(())
}

async fn blockhashes(data_dir: AkulaDataDir) -> anyhow::Result<()> {
    std::fs::create_dir_all(&data_dir.0)?;

    let etl_temp_path = data_dir.etl_temp_dir();
    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let etl_temp_dir =
        Arc::new(tempfile::tempdir_in(&etl_temp_path).context("failed to create ETL temp dir")?);

    let env = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(BlockHashes {
        temp_dir: etl_temp_dir.clone(),
    });
    staged_sync.run(&env).await?;
    Ok(())
}
fn open_db(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        CHAINDATA_TABLES.clone(),
    )
}

fn table_sizes(data_dir: AkulaDataDir, csv: bool) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let mut sizes = env.begin()?.table_sizes()?.into_iter().collect::<Vec<_>>();
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

fn db_query(data_dir: AkulaDataDir, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let value = txn.get::<Vec<u8>>(&db, &key)?;

    println!("{:?}", value.as_ref().map(hex::encode));

    Ok(())
}

fn db_walk(
    data_dir: AkulaDataDir,
    table: String,
    starting_key: Option<Bytes>,
    max_entries: Option<usize>,
) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

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

fn check_table_eq(db1_path: PathBuf, db2_path: PathBuf, table: String) -> anyhow::Result<()> {
    let env1 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db1_path,
        Default::default(),
    )?;
    let env2 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
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

fn read_block(data_dir: AkulaDataDir, block_num: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let canonical_hash = tx
        .get(tables::CanonicalHeader, block_num)?
        .ok_or_else(|| format_err!("no such canonical block"))?;
    let header = tx
        .get(tables::Header, (block_num, canonical_hash))?
        .ok_or_else(|| format_err!("header not found"))?;
    let body =
        akula::accessors::chain::block_body::read_without_senders(&tx, canonical_hash, block_num)?
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

fn read_account(
    data_dir: AkulaDataDir,
    address: Address,
    block_number: Option<BlockNumber>,
) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let account = akula::accessors::state::account::read(&tx, address, block_number)?;

    println!("{:?}", account);

    Ok(())
}

fn read_account_changes(data_dir: AkulaDataDir, block: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let walker = tx.cursor(tables::AccountChangeSet)?.walk_dup(block);

    pin!(walker);

    while let Some(tables::AccountChange { address, account }) = walker.next().transpose()? {
        println!("{:?}: {:?}", address, account);
    }

    Ok(())
}

fn read_storage(data_dir: AkulaDataDir, address: Address) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    println!(
        "{:?}",
        tx.cursor(tables::Storage)?
            .walk_dup(address)
            .collect::<anyhow::Result<Vec<_>>>()?
    );

    Ok(())
}

fn read_storage_changes(data_dir: AkulaDataDir, block: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let cur = tx.cursor(tables::StorageChangeSet)?;

    let walker = cur.walk(Some(block));

    pin!(walker);

    let mut changes = BTreeMap::<Address, BTreeMap<H256, U256>>::new();
    while let Some((
        tables::StorageChangeKey {
            block_number,
            address,
        },
        tables::StorageChange { location, value },
    )) = walker.next().transpose()?
    {
        if block_number > block {
            break;
        }

        changes.entry(address).or_default().insert(location, value);
    }

    for (address, slots) in changes {
        println!("{:?}: {:?}", address, slots);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt: Opt = Opt::parse();

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
        OptCommand::DbStats { csv } => table_sizes(opt.data_dir, csv)?,
        OptCommand::Blockhashes => blockhashes(opt.data_dir).await?,
        OptCommand::HeaderDownload { chain, uri } => {
            download_headers(opt.data_dir, chain, uri).await?
        }
        OptCommand::DbQuery { table, key } => db_query(opt.data_dir, table, key)?,
        OptCommand::DbWalk {
            table,
            starting_key,
            max_entries,
        } => db_walk(opt.data_dir, table, starting_key, max_entries)?,
        OptCommand::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table)?,
        OptCommand::ReadBlock { block_number } => read_block(opt.data_dir, block_number)?,
        OptCommand::ReadAccount {
            address,
            block_number,
        } => read_account(opt.data_dir, address, block_number)?,
        OptCommand::ReadAccountChanges { block } => read_account_changes(opt.data_dir, block)?,
        OptCommand::ReadStorage { address } => read_storage(opt.data_dir, address)?,
        OptCommand::ReadStorageChanges { block } => read_storage_changes(opt.data_dir, block)?,
    }

    Ok(())
}
