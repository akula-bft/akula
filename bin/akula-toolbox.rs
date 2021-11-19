use akula::{
    hex_to_bytes,
    kv::traits::KV,
    models::*,
    stagedsync::{self},
    stages::*,
};
use anyhow::{bail, ensure, Context};
use bytes::Bytes;
use itertools::Itertools;
use std::{borrow::Cow, path::PathBuf};
use structopt::StructOpt;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(StructOpt)]
#[structopt(name = "Akula Toolbox", about = "Utilities for Akula Ethereum client")]
pub enum Opt {
    /// Print database statistics
    DbStats {
        /// Chain data path
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
        /// Whether to print CSV
        #[structopt(long)]
        csv: bool,
    },

    /// Query database
    DbQuery {
        #[structopt(long, parse(from_os_str))]
        chaindata: PathBuf,
        #[structopt(long)]
        table: String,
        #[structopt(long, parse(try_from_str = hex_to_bytes))]
        key: Bytes,
    },

    /// Walk over table entries
    DbWalk {
        #[structopt(long, parse(from_os_str))]
        chaindata: PathBuf,
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
    Blockhashes {
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
    },
}

async fn blockhashes(chaindata: PathBuf) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_rw(
        mdbx::Environment::new(),
        &chaindata,
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?;

    let mut staged_sync = stagedsync::StagedSync::new();
    staged_sync.push(BlockHashes);
    staged_sync.run(&env).await?;
}

async fn table_sizes(chaindata: PathBuf, csv: bool) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
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

async fn db_query(chaindata: PathBuf, table: String, key: Bytes) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
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
            rlp::decode::<akula::models::Transaction>(&v)?.hash()
        );
    }

    Ok(())
}

async fn db_walk(
    chaindata: PathBuf,
    table: String,
    starting_key: Option<Bytes>,
    max_entries: Option<usize>,
) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
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
        use akula::kv::TableDecode;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        // the `TasksLayer` can be used in combination with other `tracing` layers...
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(filter)
        .init();

    match opt {
        Opt::DbStats { chaindata, csv } => table_sizes(chaindata, csv).await?,
        Opt::Blockhashes { chaindata } => blockhashes(chaindata).await?,
        Opt::DbQuery {
            chaindata,
            table,
            key,
        } => db_query(chaindata, table, key).await?,
        Opt::DbWalk {
            chaindata,
            table,
            starting_key,
            max_entries,
        } => db_walk(chaindata, table, starting_key, max_entries).await?,
        Opt::CheckEqual { db1, db2, table } => check_table_eq(db1, db2, table).await?,
    }

    Ok(())
}
