use akula::{
    akula_tracing::{self, Component},
    binutil::AkulaDataDir,
    kv::{mdbx::*, MdbxWithDirHandle},
    rpc::{
        erigon::ErigonApiServerImpl, eth::EthApiServerImpl, net::NetApiServerImpl,
        otterscan::OtterscanApiServerImpl, trace::TraceApiServerImpl, web3::Web3ApiServerImpl,
    },
};
use anyhow::format_err;
use clap::Parser;
use ethereum_jsonrpc::{
    ErigonApiServer, EthApiServer, NetApiServer, OtterscanApiServer, TraceApiServer, Web3ApiServer,
};
use jsonrpsee::{
    core::server::rpc_module::Methods, http_server::HttpServerBuilder, ws_server::WsServerBuilder,
};

use std::{collections::HashSet, future::pending, net::SocketAddr, sync::Arc};
use tracing_subscriber::prelude::*;

#[derive(Parser)]
#[clap(name = "Akula RPC", about = "RPC server for Akula")]
pub struct Opt {
    #[clap(long)]
    pub datadir: AkulaDataDir,

    #[clap(long)]
    pub listen_address: SocketAddr,

    #[clap(long)]
    pub websocket_listen_address: SocketAddr,

    #[clap(long)]
    pub grpc_listen_address: SocketAddr,

    /// Enable API options
    #[clap(long, min_values(1))]
    pub enable_api: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    akula_tracing::build_subscriber(Component::RPCDaemon).init();

    let db: Arc<MdbxWithDirHandle<NoWriteMap>> = Arc::new(
        MdbxEnvironment::<NoWriteMap>::open_ro(
            mdbx::Environment::new(),
            &opt.datadir,
            &akula::kv::tables::CHAINDATA_TABLES,
        )?
        .into(),
    );

    let network_id = akula::accessors::chain::chain_config::read(&db.begin()?)?
        .ok_or_else(|| format_err!("no chainspec found"))?
        .params
        .network_id;

    let http_server = HttpServerBuilder::default()
        .build(opt.listen_address)
        .await?;

    let websocket_server = WsServerBuilder::default()
        .build(opt.websocket_listen_address)
        .await?;

    let mut api = Methods::new();

    let api_options = opt
        .enable_api
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect::<HashSet<String>>();

    if !api_options.is_empty() && api_options.contains("eth") {
        api.merge(
            EthApiServerImpl {
                db: db.clone(),
                call_gas_limit: 100_000_000,
            }
            .into_rpc(),
        )
        .unwrap();
    }

    if !api_options.is_empty() && api_options.contains("net") {
        api.merge(NetApiServerImpl { network_id }.into_rpc())
            .unwrap();
    }

    if !api_options.is_empty() && api_options.contains("erigon") {
        api.merge(ErigonApiServerImpl { db: db.clone() }.into_rpc())
            .unwrap();
    }

    if !api_options.is_empty() && api_options.contains("otterscan") {
        api.merge(OtterscanApiServerImpl { db: db.clone() }.into_rpc())
            .unwrap();
    }

    if !api_options.is_empty() && api_options.contains("trace") {
        api.merge(
            TraceApiServerImpl {
                db: db.clone(),
                call_gas_limit: 100_000_000,
            }
            .into_rpc(),
        )
        .unwrap();
    }

    if !api_options.is_empty() && api_options.contains("web3") {
        api.merge(Web3ApiServerImpl.into_rpc()).unwrap();
    }

    let _http_server_handle = http_server.start(api.clone())?;
    let _websocket_server_handle = websocket_server.start(api)?;

    tokio::spawn({
        let db = db.clone();
        let listen_address = opt.grpc_listen_address;
        async move {
            tonic::transport::Server::builder()
                .add_service(
                    ethereum_interfaces::web3::trace_api_server::TraceApiServer::new(
                        TraceApiServerImpl {
                            db,
                            call_gas_limit: 100_000_000,
                        },
                    ),
                )
                .serve(listen_address)
                .await
                .unwrap();
        }
    });

    pending().await
}
