use akula::table_sizes;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = "Akula",
    about = "Ethereum client based on turbo-geth architecture"
)]
pub enum Opt {
    /// Print database statistics
    DbStats {
        /// Chain data path
        #[structopt(parse(from_os_str))]
        chaindata: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    match opt {
        Opt::DbStats { chaindata } => {
            let env = akula::Environment::open_ro(
                mdbx::Environment::new(),
                &chaindata,
                &akula::tables::TABLE_MAP,
            )?;
            let mut sizes = table_sizes(&env.begin_ro_txn()?)?
                .into_iter()
                .collect::<Vec<_>>();
            sizes.sort_by_key(|(_, size)| *size);
            for (table, size) in &sizes {
                println!("{} - {}", table, bytesize::ByteSize::b(*size));
            }
            println!(
                "TOTAL: {}",
                bytesize::ByteSize::b(sizes.into_iter().map(|(_, size)| size).sum())
            );
        }
    }

    Ok(())
}
