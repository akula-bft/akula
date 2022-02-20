#![allow(dead_code, clippy::upper_case_acronyms)]

use akula::sentry::{devp2p::*, eth::*, services::*, CapabilityServerImpl};
use anyhow::Context;
use cidr::IpCidr;
use clap::Parser;
use derive_more::FromStr;
use educe::Educe;
use ethereum_interfaces::sentry::sentry_server::SentryServer;
use maplit::btreemap;
use secp256k1::{PublicKey, SecretKey, SECP256K1};
use std::{
    collections::HashMap, fmt::Debug, path::PathBuf, str::FromStr, sync::Arc, time::Duration,
};
use task_group::TaskGroup;
use tokio::time::sleep;
use tokio_stream::StreamMap;
use tonic::transport::Server;
use tracing::*;
use tracing_subscriber::{prelude::*, EnvFilter};
use trust_dns_resolver::TokioAsyncResolver;

pub const BOOTNODES: &[&str] = &[
	"enode://d860a01f9722d78051619d1e2351aba3f43f943f6f00718d1b9baa4101932a1f5011f16bb2b1bb35db20d6fe28fa0bf09636d26a87d31de9ec6203eeedb1f666@18.138.108.67:30303",   // bootnode-aws-ap-southeast-1-001
	"enode://22a8232c3abc76a16ae9d6c3b164f98775fe226f0917b0ca871128a74a8e9630b458460865bab457221f1d448dd9791d24c4e5d88786180ac185df813a68d4de@3.209.45.79:30303",     // bootnode-aws-us-east-1-001
	"enode://ca6de62fce278f96aea6ec5a2daadb877e51651247cb96ee310a318def462913b653963c155a0ef6c7d50048bba6e6cea881130857413d9f50a621546b590758@34.255.23.113:30303",   // bootnode-aws-eu-west-1-001
	"enode://279944d8dcd428dffaa7436f25ca0ca43ae19e7bcf94a8fb7d1641651f92d121e972ac2e8f381414b80cc8e5555811c2ec6e1a99bb009b3f53c4c69923e11bd8@35.158.244.151:30303",  // bootnode-aws-eu-central-1-001
	"enode://8499da03c47d637b20eee24eec3c356c9a2e6148d6fe25ca195c7949ab8ec2c03e3556126b0d7ed644675e78c4318b08691b7b57de10e5f0d40d05b09238fa0a@52.187.207.27:30303",   // bootnode-azure-australiaeast-001
	"enode://103858bdb88756c71f15e9b5e09b56dc1be52f0a5021d46301dbbfb7e130029cc9d0d6f73f693bc29b665770fff7da4d34f3c6379fe12721b5d7a0bcb5ca1fc1@191.234.162.198:30303", // bootnode-azure-brazilsouth-001
	"enode://715171f50508aba88aecd1250af392a45a330af91d7b90701c436b618c86aaa1589c9184561907bebbb56439b8f8787bc01f49a7c77276c58c1b09822d75e8e8@52.231.165.108:30303",  // bootnode-azure-koreasouth-001
	"enode://5d6d7cd20d6da4bb83a1d28cadb5d409b64edf314c0335df658c1a54e32c7c4a7ab7823d57c39b6a757556e68ff1df17c748b698544a55cb488b52479a92b60f@104.42.217.25:30303",   // bootnode-azure-westus-001
];

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
    #[clap(long, default_value = "20")]
    pub discv4_cache: usize,
    #[clap(long, default_value = "1")]
    pub discv4_concurrent_lookups: usize,
    #[clap(long)]
    pub static_peers: Vec<NR>,
    #[clap(long, default_value = "5000")]
    pub static_peers_interval: u64,
    #[clap(long, default_value = "50")]
    pub max_peers: usize,
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

#[derive(Debug, Educe)]
#[educe(Default)]
pub struct DnsDiscConfig {
    #[educe(Default("all.mainnet.ethdisco.net"))]
    pub address: String,
}

#[derive(Debug, FromStr)]
pub struct NR(pub NodeRecord);

#[derive(Debug, FromStr)]
pub struct Discv4NR(pub akula::sentry::devp2p::disc::v4::NodeRecord);

struct OptsDnsDisc {
    address: String,
}

impl OptsDnsDisc {
    fn make_task(self) -> anyhow::Result<DnsDiscovery> {
        info!("Starting DNS discovery fetch from {}", self.address);

        let dns_resolver = akula::sentry::devp2p::disc::dns::Resolver::new(Arc::new(
            TokioAsyncResolver::tokio_from_system_conf().context("Failed to start DNS resolver")?,
        ));

        let task = DnsDiscovery::new(Arc::new(dns_resolver), self.address, None);

        Ok(task)
    }
}

struct OptsDiscV4 {
    discv4_port: u16,
    discv4_bootnodes: Vec<Discv4NR>,
    discv4_cache: usize,
    discv4_concurrent_lookups: usize,
    listen_port: u16,
}

impl OptsDiscV4 {
    async fn make_task(self, secret_key: &SecretKey) -> anyhow::Result<Discv4> {
        info!("Starting discv4 at port {}", self.discv4_port);

        let mut bootstrap_nodes = self
            .discv4_bootnodes
            .into_iter()
            .map(|Discv4NR(nr)| nr)
            .collect::<Vec<_>>();

        if bootstrap_nodes.is_empty() {
            bootstrap_nodes = BOOTNODES
                .iter()
                .map(|b| Ok(Discv4NR::from_str(b)?.0))
                .collect::<Result<Vec<_>, <Discv4NR as FromStr>::Err>>()?;
            info!("Using default discv4 bootstrap nodes");
        }

        let node = akula::sentry::devp2p::disc::v4::Node::new(
            format!("0.0.0.0:{}", self.discv4_port).parse().unwrap(),
            *secret_key,
            bootstrap_nodes,
            None,
            true,
            self.listen_port,
        )
        .await?;

        let task = Discv4Builder::default()
            .with_cache(self.discv4_cache)
            .with_concurrent_lookups(self.discv4_concurrent_lookups)
            .build(node);

        Ok(task)
    }
}

struct OptsDiscStatic {
    static_peers: Vec<NR>,
    static_peers_interval: u64,
}

impl OptsDiscStatic {
    fn make_task(self) -> anyhow::Result<StaticNodes> {
        info!("Enabling static peers: {:?}", self.static_peers);

        let task = StaticNodes::new(
            self.static_peers
                .iter()
                .map(|&NR(NodeRecord { addr, id })| (addr, id))
                .collect::<HashMap<_, _>>(),
            Duration::from_millis(self.static_peers_interval),
        );
        Ok(task)
    }
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
            discv4_bootnodes: opts.discv4_bootnodes,
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
        .with_listen_options(ListenOptions {
            discovery_tasks,
            max_peers: opts.max_peers,
            addr: listen_addr.parse().unwrap(),
            cidr: opts.cidr,
            no_new_peers,
        })
        .with_client_version(format!("sentry/v{}", env!("CARGO_PKG_VERSION")))
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
