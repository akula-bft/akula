use akula::{
    stagedsync::{self},
    table_sizes,
};
use std::{path::PathBuf, time::Duration};
use structopt::StructOpt;
use tokio::time::sleep;

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
            let _ = sentry_addr;
            let db = akula::new_mem_database()?;

            let mut staged_sync = stagedsync::StagedSync::new(|| async move {
                sleep(Duration::from_millis(6000)).await;
            });
            staged_sync.push(akula::stages::HeaderDownload);
            // staged_sync.push(akula::stages::BlockHashes);
            // staged_sync.push(akula::stages::ExecutionStage);

            // stagedsync::StagedSync::new(vec![], vec![]);
            staged_sync.run(&db).await?;
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
