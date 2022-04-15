#![allow(dead_code, clippy::upper_case_acronyms)]
use akula::{
    models::ChainConfig,
    sentry::{devp2p::*, eth::*, opts::*, services::*, CapabilityServerImpl},
    version_string,
};
use anyhow::Context;
use cidr::IpCidr;
use clap::Parser;
use educe::Educe;
use ethereum_interfaces::sentry::sentry_server::SentryServer;
use maplit::btreemap;
use secp256k1::{PublicKey, SecretKey, SECP256K1};
use std::{num::NonZeroUsize, path::PathBuf, sync::Arc, time::Duration};
use task_group::TaskGroup;
use tokio::time::sleep;
use tokio_stream::StreamMap;
use tonic::transport::Server;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Educe, Parser)]
#[clap(
    name = "ethereum-sentry",
    about = "Service that listens to Ethereum's P2P network, serves information to other nodes, and provides gRPC interface to clients to interact with the network."
)]
#[educe(Debug)]
pub struct Opts {
    #[clap(long)]
    #[educe(Debug(ignore))]
    pub node_key: Option<String>,
    #[clap(long, default_value = "30303")]
    pub listen_port: u16,
    #[clap(long)]
    pub cidr: Option<IpCidr>,
    #[clap(long, default_value = "127.0.0.1:8000")]
    pub sentry_addr: String,
    #[clap(long, default_value = "all.mainnet.ethdisco.net")]
    pub dnsdisc_address: String,
    #[clap(long, default_value = "30303")]
    pub discv4_port: u16,
    #[clap(long)]
    pub discv4_bootnodes: Vec<Discv4NR>,
    #[clap(long, default_value = "1000")]
    pub discv4_cache: usize,
    #[clap(long, default_value = "1")]
    pub discv4_concurrent_lookups: usize,
    #[clap(long)]
    pub static_peers: Vec<NR>,
    #[clap(long, default_value = "5000")]
    pub static_peers_interval: u64,
    #[clap(long, default_value = "50")]
    pub max_peers: NonZeroUsize,
    /// Disable DNS and UDP discovery, only use static peers.
    #[clap(long, takes_value = false)]
    pub no_discovery: bool,
    /// Disable DNS discovery
    #[clap(long, takes_value = false)]
    pub no_dns_discovery: bool,
    #[clap(long)]
    pub peers_file: Option<PathBuf>,
    #[clap(long, takes_value = false)]
    pub tokio_console: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    fdlimit::raise_fd_limit();

    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new(
            "akula_sentry=info,akula::sentry::devp2p=info,akula::sentry::devp2p::disc=info",
        )
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();

    let secret_key;
    if let Some(data) = opts.node_key {
        secret_key = SecretKey::from_slice(&hex::decode(data)?)?;
        info!("Loaded node key from config");
    } else {
        secret_key = SecretKey::new(&mut secp256k1::rand::thread_rng());
        info!("Generated new node key: {:?}", secret_key);
    };

    let listen_addr = format!("0.0.0.0:{}", opts.listen_port);

    info!("Starting Ethereum sentry");

    info!(
        "Node ID: {}",
        hex::encode(
            akula::sentry::devp2p::util::pk2id(&PublicKey::from_secret_key(SECP256K1, &secret_key))
                .as_bytes()
        )
    );

    if let Some(cidr_filter) = &opts.cidr {
        info!("Peers restricted to range {}", cidr_filter);
    }

    let mut discovery_tasks: StreamMap<String, Discovery> = StreamMap::new();

    let bootnodes = if opts.discv4_bootnodes.is_empty() {
        ChainConfig::new("mainnet")
            .unwrap()
            .bootnodes()
            .iter()
            .map(|b| Discv4NR(b.parse().unwrap()))
            .collect::<Vec<_>>()
    } else {
        opts.discv4_bootnodes
    };

    if !opts.no_discovery {
        if !opts.no_dns_discovery {
            let task_opts = OptsDnsDisc {
                address: opts.dnsdisc_address,
            };
            let task = task_opts.make_task()?;
            discovery_tasks.insert("dnsdisc".to_string(), Box::pin(task));
        }

        let task_opts = OptsDiscV4 {
            discv4_port: opts.discv4_port,
            discv4_bootnodes: bootnodes,
            discv4_cache: opts.discv4_cache,
            discv4_concurrent_lookups: opts.discv4_concurrent_lookups,
            listen_port: opts.listen_port,
        };
        let task = task_opts.make_task(&secret_key).await?;
        discovery_tasks.insert("discv4".to_string(), Box::pin(task));
    }

    if !opts.static_peers.is_empty() {
        let task_opts = OptsDiscStatic {
            static_peers: opts.static_peers,
            static_peers_interval: opts.static_peers_interval,
        };
        let task = task_opts.make_task()?;
        discovery_tasks.insert("static peers".to_string(), Box::pin(task));
    }

    if discovery_tasks.is_empty() {
        warn!("All discovery methods are disabled, sentry will not search for peers.");
    }

    let tasks = Arc::new(TaskGroup::new());

    let protocol_version = EthProtocolVersion::Eth66;

    let capability_server = Arc::new(CapabilityServerImpl::new(protocol_version, opts.max_peers));

    let no_new_peers = capability_server.no_new_peers_handle();

    let swarm = Swarm::builder()
        .with_task_group(tasks.clone())
        .with_listen_options(ListenOptions::new(
            discovery_tasks,
             opts.max_peers,
             listen_addr.parse().unwrap(),
            opts.cidr,
            no_new_peers,
        ))
        .with_client_version(version_string())
        .build(
            btreemap! {
                CapabilityId { name: capability_name(), version: protocol_version as CapabilityVersion } => 17,
            },
            capability_server.clone(),
            secret_key,
        )
        .await
        .context("Failed to start RLPx node")?;

    info!("RLPx node listening at {}", listen_addr);

    let sentry_addr = opts.sentry_addr.parse()?;
    tasks.spawn(async move {
        let svc = SentryServer::new(SentryService::new(capability_server));

        info!("Sentry gRPC server starting on {}", sentry_addr);

        Server::builder()
            .add_service(svc)
            .serve(sentry_addr)
            .await
            .unwrap();
    });

    loop {
        info!(
            "Peer info: {} active (+{} dialing) / {} max.",
            swarm.connected_peers(),
            swarm.dialing(),
            opts.max_peers
        );

        sleep(Duration::from_secs(5)).await;
    }
}
