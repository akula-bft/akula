use akula::{
    binutil::AkulaDataDir,
    kv::{mdbx::*, MdbxWithDirHandle},
    rpc::{
        erigon::ErigonApiServerImpl, eth::EthApiServerImpl, net::NetApiServerImpl,
        otterscan::OtterscanApiServerImpl,
    },
};
use clap::Parser;
use ethereum_jsonrpc::{ErigonApiServer, EthApiServer, NetApiServer, OtterscanApiServer};
use jsonrpsee::{core::server::rpc_module::Methods, http_server::HttpServerBuilder};
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

    let db: Arc<MdbxWithDirHandle<NoWriteMap>> = Arc::new(
        MdbxEnvironment::<NoWriteMap>::open_ro(
            mdbx::Environment::new(),
            &opt.datadir,
            akula::kv::tables::CHAINDATA_TABLES.clone(),
        )?
        .into(),
    );

    let server = HttpServerBuilder::default()
        .build(opt.listen_address)
        .await?;

    let mut api = Methods::new();
    api.merge(
        EthApiServerImpl {
            db: db.clone(),
            call_gas_limit: 100_000_000,
        }
        .into_rpc(),
    )
    .unwrap();
    api.merge(NetApiServerImpl.into_rpc()).unwrap();
    api.merge(ErigonApiServerImpl { db: db.clone() }.into_rpc())
        .unwrap();
    api.merge(OtterscanApiServerImpl { db }.into_rpc()).unwrap();

    let _server_handle = server.start(api)?;

    pending().await
}
