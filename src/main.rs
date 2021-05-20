use akula::{stagedsync, table_sizes};
use std::{future::ready, path::PathBuf, sync::Arc};
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "Akula",
    about = "Ethereum client based on turbo-geth architecture"
)]
pub enum Opt {
    /// Run Akula core
    Core {
        #[structopt(long)]
        sentry_addr: String,
    },
    /// Print database statistics
    DbStats {
        /// Chain data path
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
        /// Whether to print CSV
        #[structopt(long)]
        csv: bool,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::Core { sentry_addr } => {
            let mut db = akula::new_mem_database()?;

            struct HelloStage;
            struct EthereumStage;
            struct WorldStage;

            let mut staged_sync = stagedsync::StagedSync::new(|| ready(()));
            // staged_sync.push(HelloStage);
            // staged_sync.push(EthereumStage);
            // staged_sync.push(WorldStage);

            // stagedsync::StagedSync::new(vec![], vec![]);
            staged_sync.run(&db);
        }
        Opt::DbStats { chaindata, csv } => {
            let env = akula::Environment::open_ro(
                mdbx::Environment::new(),
                &chaindata,
                &akula::tables::TABLE_MAP,
            )?;
            let mut sizes = table_sizes(&env.begin_ro_txn()?)?
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
        }
    }

    Ok(())
}
