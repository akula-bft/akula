use akula::{
    binutil::AkulaDataDir,
    kv::{tables, traits::*},
    models::*,
    stagedsync::stages::*,
};
use async_trait::async_trait;
use clap::Parser;
use ethnum::U256;
use jsonrpsee::{core::RpcResult, http_server::HttpServerBuilder, proc_macros::rpc};
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

#[rpc(server, namespace = "eth")]
pub trait EthApi {
    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<BlockNumber>;
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256>;
}

pub struct EthApiServerImpl<DB>
where
    DB: KV,
{
    db: Arc<DB>,
}

#[async_trait]
impl<DB> EthApiServer for EthApiServerImpl<DB>
where
    DB: KV,
{
    async fn block_number(&self) -> RpcResult<BlockNumber> {
        Ok(self
            .db
            .begin()
            .await?
            .get(tables::SyncStage, FINISH)
            .await?
            .unwrap_or(BlockNumber(0)))
    }

    async fn get_balance(&self, address: Address, block_number: BlockNumber) -> RpcResult<U256> {
        Ok(akula::accessors::state::account::read(
            &self.db.begin().await?,
            address,
            Some(block_number),
        )
        .await?
        .map(|acc| acc.balance)
        .unwrap_or(U256::ZERO))
    }
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
    let _server_handle = server.start(EthApiServerImpl { db }.into_rpc())?;

    pending().await
}
