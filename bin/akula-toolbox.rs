use akula::{kv::traits::KV, stagedsync};
use std::path::PathBuf;
use structopt::StructOpt;

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
    staged_sync.push(akula::stages::BlockHashes);
    staged_sync.run(&env).await?;
}

async fn table_sizes(chaindata: PathBuf, csv: bool) -> anyhow::Result<()> {
    let env = akula::MdbxEnvironment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &chaindata,
        Default::default(),
    )?;
    let mut sizes = env
        .begin(0)
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::DbStats { chaindata, csv } => table_sizes(chaindata, csv).await?,
        Opt::Blockhashes { chaindata } => blockhashes(chaindata).await?,
    }

    Ok(())
}
