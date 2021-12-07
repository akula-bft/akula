use crate::sentry::sentry_address::SentryAddress;
use anyhow::format_err;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
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
    #[structopt(
        long = "downloader.headers-batch-size",
        help = "How many headers to download per stage run.",
        default_value = "100000"
    )]
    pub headers_batch_size: usize,
}

impl Opts {
    pub fn new(args_opt: Option<Vec<String>>, chain_names: &[&str]) -> anyhow::Result<Self> {
        let instance: Opts = match args_opt {
            Some(args) => Opts::from_iter_safe(args)?,
            None => Opts::from_args_safe()?,
        };

        Self::validate_chain_name(&instance.chain_name, chain_names)?;

        Ok(instance)
    }

    pub fn validate_chain_name(chain_name: &str, chain_names: &[&str]) -> anyhow::Result<()> {
        if chain_names.contains(&chain_name) {
            Ok(())
        } else {
            Err(format_err!("unknown chain '{}'", chain_name))
        }
    }
}
