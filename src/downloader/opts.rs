use crate::sentry::sentry_address::SentryAddress;
use anyhow::format_err;
use directories::ProjectDirs;
use std::str::FromStr;
use structopt::StructOpt;

pub struct DataDirOpt(pub std::path::PathBuf);

#[derive(StructOpt)]
#[structopt(name = "ddl", about = "test sentry client")]
pub struct Opts {
    #[structopt(
        long = "sentry.api.addr",
        help = "Sentry GRPC service URL as 'http://host:port'",
        default_value = "http://localhost:8000"
    )]
    pub sentry_api_addr: SentryAddress,
    #[structopt(
        long = "chain",
        help = "Name of the testnet to join",
        default_value = "mainnet"
    )]
    pub chain_name: String,
    #[structopt(long = "datadir", help = "Database directory path", default_value)]
    pub data_dir: DataDirOpt,
}

impl Opts {
    pub fn new(args_opt: Option<Vec<String>>, chain_names: &[&str]) -> anyhow::Result<Self> {
        let instance: Opts = match args_opt {
            Some(args) => Opts::from_iter_safe(args)?,
            None => Opts::from_args_safe()?,
        };

        if !chain_names.contains(&instance.chain_name.as_str()) {
            return Err(format_err!("unknown chain '{}'", instance.chain_name));
        }

        Ok(instance)
    }
}

impl Default for DataDirOpt {
    fn default() -> Self {
        if let Some(dirs) = ProjectDirs::from("com", "akula-bft", "akula") {
            DataDirOpt(dirs.data_dir().to_path_buf())
        } else {
            DataDirOpt(std::path::PathBuf::from("data"))
        }
    }
}

impl ToString for DataDirOpt {
    fn to_string(&self) -> String {
        String::from(self.0.as_os_str().to_str().unwrap())
    }
}

impl FromStr for DataDirOpt {
    type Err = anyhow::Error;

    fn from_str(path_str: &str) -> anyhow::Result<Self> {
        Ok(DataDirOpt(std::path::PathBuf::from(path_str)))
    }
}
