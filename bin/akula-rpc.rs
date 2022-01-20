use akula::{binutil::AkulaDataDir, rpc::EthApiServer};
use clap::Parser;
use jsonrpsee::http_server::HttpServerBuilder;
use std::{future::pending, net::SocketAddr, sync::Arc};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Akula RPC", about = "RPC server for Akula")]
pub struct Opt {
    #[clap(long)]
    pub datadir: AkulaDataDir,

    #[clap(long)]
    pub listen_address: SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info,rpc=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    let db = Arc::new(akula::kv::mdbx::Environment::<mdbx::NoWriteMap>::open_ro(
        mdbx::Environment::new(),
        &opt.datadir,
        akula::kv::tables::CHAINDATA_TABLES.clone(),
    )?);

    let server = HttpServerBuilder::default().build(opt.listen_address)?;
    let _server_handle = server.start(akula::rpc::EthApiServerImpl { db }.into_rpc())?;

    pending().await
}
