#![feature(never_type)]
use akula::{
    binutil::AkulaDataDir,
    consensus::{engine_factory, Consensus, ForkChoiceMode},
    hex_to_bytes,
    kv::{
        tables::{self, BitmapKey, CHAINDATA_TABLES},
        traits::*,
    },
    models::*,
    p2p::node::NodeBuilder,
    stagedsync,
    stages::*,
};
use anyhow::{ensure, format_err, Context};
use bytes::Bytes;
use clap::Parser;
use expanded_pathbuf::ExpandedPathBuf;
use std::{borrow::Cow, collections::BTreeMap, io::Read, sync::Arc};
use tokio::pin;
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
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        starting_key: Option<Bytes>,
        #[clap(long)]
        max_entries: Option<usize>,
    },

    /// Set db value
    DbSet {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
        #[clap(long, value_parser(hex_to_bytes))]
        value: Bytes,
    },

    /// Unset db value
    DbUnset {
        #[clap(long)]
        table: String,
        #[clap(long, value_parser(hex_to_bytes))]
        key: Bytes,
    },

    /// Drop db
    DbDrop {
        #[clap(long)]
        table: String,
    },

    /// Check table equality in two databases
    CheckEqual {
        #[clap(long)]
        db1: ExpandedPathBuf,
        #[clap(long)]
        db2: ExpandedPathBuf,
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

    ReadAccountChangedBlocks {
        address: Address,
    },

    ReadStorage {
        address: Address,
    },

    ReadStorageChanges {
        block: BlockNumber,
    },

    /// Overwrite chainspec in database with user-provided one
    OverwriteChainspec {
        chainspec_file: ExpandedPathBuf,
    },

    SetStageProgress {
        #[clap(long)]
        stage: String,
        #[clap(long)]
        progress: BlockNumber,
    },

    UnsetStageProgress {
        #[clap(long)]
        stage: String,
    },
}

async fn download_headers(
    data_dir: AkulaDataDir,
    chain: String,
    uri: tonic::transport::Uri,
) -> anyhow::Result<()> {
    let chain_config = ChainConfig::new(chain.as_ref())?;

    let chain_data_dir = data_dir.chain_data_dir();
    let etl_temp_path = data_dir.etl_temp_dir();

    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let env = Arc::new(akula::kv::new_database(&CHAINDATA_TABLES, &chain_data_dir)?);
    let consensus: Arc<dyn Consensus> =
        engine_factory(Some(env.clone()), chain_config.chain_spec.clone(), None)?.into();
    let txn = env.begin_mutable()?;
    akula::genesis::initialize_genesis(
        &txn,
        &Arc::new(tempfile::tempdir_in(etl_temp_path).context("failed to create ETL temp dir")?),
        true,
        Some(chain_config.chain_spec.clone()),
    )?;

    txn.commit()?;

    let node = Arc::new(
        NodeBuilder::new(chain_config)
            .set_stash(env.clone())
            .add_sentry(uri)
            .build()?,
    );
    tokio::spawn({
        let node = node.clone();
        let tip_discovery = !matches!(consensus.fork_choice_mode(), ForkChoiceMode::External(_));
        async move {
            node.start_sync(tip_discovery).await.unwrap();
        }
    });

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(
        HeaderDownload {
            node,
            consensus,
            max_block: u64::MAX.into(),
            increment: None,
        },
        false,
    );
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
        &akula::kv::tables::CHAINDATA_TABLES,
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(
        BlockHashes {
            temp_dir: etl_temp_dir.clone(),
        },
        false,
    );
    staged_sync.run(&env).await?;
    Ok(())
}
fn open_db(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &CHAINDATA_TABLES,
    )
}

fn open_db_rw(
    data_dir: AkulaDataDir,
) -> anyhow::Result<akula::kv::mdbx::MdbxEnvironment<mdbx::NoWriteMap>> {
    akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &data_dir.chain_data_dir(),
        &CHAINDATA_TABLES,
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

fn decode_db_inner<T: Table, K: TableDecode>(
    _: T,
    key: &[u8],
    value: &[u8],
) -> anyhow::Result<(K, T::Value)> {
    Ok((
        <K as TableDecode>::decode(key)?,
        <T::Value as TableDecode>::decode(value)?,
    ))
}

fn decode_db<T: Table>(table: T, key: &[u8], value: &[u8]) -> anyhow::Result<(T::Key, T::Value)>
where
    T::Key: TableDecode,
{
    decode_db_inner::<T, T::Key>(table, key, value)
}

macro_rules! select_db_from_str {
    ($name:expr, [$($table:ident),* $(,)?], $fn:expr, $ow:expr) => {
        match $name {
            $(
                stringify!($table) => $fn($table),
            )*
            _ => $ow,
        }
    };
}

fn select_db_decode(table: &str, key: &[u8], value: &[u8]) -> anyhow::Result<(String, String)> {
    use akula::kv::tables::*;
    select_db_from_str!(
        table,
        [
            Account,
            Storage,
            AccountChangeSet,
            StorageChangeSet,
            HashedAccount,
            HashedStorage,
            AccountHistory,
            StorageHistory,
            Code,
            TrieAccount,
            TrieStorage,
            HeaderNumber,
            CanonicalHeader,
            Header,
            HeadersTotalDifficulty,
            BlockBody,
            BlockTransaction,
            TotalGas,
            TotalTx,
            LogAddressIndex,
            LogAddressesByBlock,
            LogTopicIndex,
            LogTopicsByBlock,
            CallTraceSet,
            CallFromIndex,
            CallToIndex,
            BlockTransactionLookup,
            Config,
            TxSender,
            Issuance,
            Version,
        ],
        |table| decode_db(table, key, value).map(|(k, v)| (format!("{:?}", k), format!("{:?}", v))),
        select_db_from_str!(
            table,
            [SyncStage, PruneProgress],
            |table| decode_db_inner::<_, Vec<u8>>(table, key, value)
                .map(|(k, v)| (String::from_utf8_lossy(&k).to_string(), format!("{:?}", v))),
            anyhow::bail!("unknown table format {}", table)
        )
    )
}

fn db_query(data_dir: AkulaDataDir, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let txn = env.begin_ro_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    let value = txn.get::<Vec<u8>>(&db, &key)?;

    println!("key:     {}", hex::encode(&key));
    println!("value:   {:?}", value.as_ref().map(hex::encode));
    if let Some(value) = &value {
        if let Ok((k, v)) = select_db_decode(&table, &key, value) {
            println!("decoded: {} => {}", k, v);
        }
    }

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
            "{} / {:?} / {:?} / {:?}",
            i,
            hex::encode(&k),
            hex::encode(&v),
            select_db_decode(&table, &k, &v),
        );
    }

    Ok(())
}

fn db_set(
    data_dir: AkulaDataDir,
    table: String,
    key: Bytes,
    value: Option<Bytes>,
) -> anyhow::Result<()> {
    let env = open_db_rw(data_dir)?;

    let txn = env.begin_rw_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    if let Some(value) = value {
        txn.put(&db, key, value, Default::default())?;
    } else {
        txn.del(&db, key, None)?;
    }
    txn.commit()?;

    Ok(())
}

fn db_drop(data_dir: AkulaDataDir, table: String) -> anyhow::Result<()> {
    let env = open_db_rw(data_dir)?;

    let txn = env.begin_rw_txn()?;
    let db = txn
        .open_db(Some(&table))
        .with_context(|| format!("failed to open table: {}", table))?;
    unsafe {
        txn.drop_db(db)?;
    }
    txn.commit()?;

    Ok(())
}

fn check_table_eq(
    db1_path: ExpandedPathBuf,
    db2_path: ExpandedPathBuf,
    table: String,
) -> anyhow::Result<()> {
    let env1 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db1_path,
        &CHAINDATA_TABLES,
    )?;
    let env2 = akula::kv::mdbx::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &db2_path,
        &CHAINDATA_TABLES,
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

    let mut kv1 = cur1.next::<Cow<[u8]>, Cow<[u8]>>()?;
    let mut kv2 = cur2.next::<Cow<[u8]>, Cow<[u8]>>()?;
    loop {
        match (&kv1, &kv2) {
            (None, None) => break,
            (None, Some((k2, v2))) => {
                info!("Missing in db1: {} [{}]", hex::encode(k2), hex::encode(v2));

                kv2 = cur2.next()?;
            }
            (Some((k1, v1)), None) => {
                info!("Missing in db2: {} [{}]", hex::encode(k1), hex::encode(v1));

                kv1 = cur1.next()?;
            }
            (Some((k1, v1)), Some((k2, v2))) => match k1.cmp(k2) {
                std::cmp::Ordering::Greater => {
                    info!("Missing in db1: {} [{}]", hex::encode(k2), hex::encode(v2));

                    kv2 = cur2.next()?;
                }
                std::cmp::Ordering::Less => {
                    info!("Missing in db2: {} [{}]", hex::encode(k1), hex::encode(v1));

                    kv1 = cur1.next()?;
                }
                std::cmp::Ordering::Equal => {
                    if v1 != v2 {
                        info!(
                            "Mismatch for key: {} [{} != {}]",
                            hex::encode(k1),
                            hex::encode(v1),
                            hex::encode(v2)
                        );
                    }

                    kv1 = cur1.next()?;
                    kv2 = cur2.next()?;
                }
            },
        }
    }

    Ok(())
}

fn read_block(data_dir: AkulaDataDir, block_num: BlockNumber) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let header = akula::accessors::chain::header::read(&tx, block_num)?
        .ok_or_else(|| format_err!("header not found"))?;
    let body = akula::accessors::chain::block_body::read_without_senders(&tx, block_num)?
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

    let walker = tx.cursor(tables::AccountChangeSet)?.walk_dup(block, None);

    pin!(walker);

    while let Some(tables::AccountChange { address, account }) = walker.next().transpose()? {
        println!("{:?}: {:?}", address, account);
    }

    Ok(())
}

fn read_account_changed_blocks(data_dir: AkulaDataDir, address: Address) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    let walker = tx.cursor(tables::AccountHistory)?.walk(Some(BitmapKey {
        inner: address,
        block_number: BlockNumber(0),
    }));

    pin!(walker);

    while let Some((key, bitmap)) = walker.next().transpose()? {
        if key.inner != address {
            break;
        }

        println!("up to {}: {:?}", key.block_number, bitmap);
    }

    Ok(())
}

fn read_storage(data_dir: AkulaDataDir, address: Address) -> anyhow::Result<()> {
    let env = open_db(data_dir)?;

    let tx = env.begin()?;

    println!(
        "{:?}",
        tx.cursor(tables::Storage)?
            .walk_dup(address, None)
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

fn overwrite_chainspec(
    data_dir: AkulaDataDir,
    chainspec_file: ExpandedPathBuf,
) -> anyhow::Result<()> {
    let mut s = String::new();
    std::fs::File::open(&chainspec_file)?.read_to_string(&mut s)?;
    let new_chainspec = TableDecode::decode(s.as_bytes())?;

    let chain_data_dir = data_dir.chain_data_dir();

    let env = Arc::new(akula::kv::new_database(&CHAINDATA_TABLES, &chain_data_dir)?);

    let tx = env.begin_mutable()?;

    tx.set(tables::Config, (), new_chainspec)?;
    tx.commit()?;

    Ok(())
}

fn set_stage_progress(
    data_dir: AkulaDataDir,
    stage: String,
    stage_progress: Option<BlockNumber>,
) -> anyhow::Result<()> {
    db_set(
        data_dir,
        tables::SyncStage::const_db_name().to_string(),
        stage.as_bytes().to_vec().into(),
        stage_progress.map(|v| v.encode().to_vec().into()),
    )
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
        OptCommand::DbSet { table, key, value } => db_set(opt.data_dir, table, key, Some(value))?,
        OptCommand::DbUnset { table, key } => db_set(opt.data_dir, table, key, None)?,
        OptCommand::DbDrop { table } => db_drop(opt.data_dir, table)?,
        OptCommand::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table)?,
        OptCommand::ReadBlock { block_number } => read_block(opt.data_dir, block_number)?,
        OptCommand::ReadAccount {
            address,
            block_number,
        } => read_account(opt.data_dir, address, block_number)?,
        OptCommand::ReadAccountChanges { block } => read_account_changes(opt.data_dir, block)?,
        OptCommand::ReadAccountChangedBlocks { address } => {
            read_account_changed_blocks(opt.data_dir, address)?
        }
        OptCommand::ReadStorage { address } => read_storage(opt.data_dir, address)?,
        OptCommand::ReadStorageChanges { block } => read_storage_changes(opt.data_dir, block)?,
        OptCommand::OverwriteChainspec { chainspec_file } => {
            overwrite_chainspec(opt.data_dir, chainspec_file)?
        }
        OptCommand::SetStageProgress { stage, progress } => {
            set_stage_progress(opt.data_dir, stage, Some(progress))?
        }
        OptCommand::UnsetStageProgress { stage } => set_stage_progress(opt.data_dir, stage, None)?,
    }

    Ok(())
}
