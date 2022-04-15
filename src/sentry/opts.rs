use crate::sentry::{
    devp2p::{
        disc::{dns::Resolver, v4::Node},
        *,
    },
    Discv4, Discv4Builder, DnsDiscovery, StaticNodes,
};
use anyhow::format_err;
use derive_more::FromStr;
use educe::Educe;
use secp256k1::SecretKey;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::info;
use trust_dns_resolver::TokioAsyncResolver;

pub const DNS_DISC_ADDRESS: &str = "all.mainnet.ethdisco.net";

#[derive(Debug, Educe)]
#[educe(Default)]
pub struct DnsDiscConfig {
    #[educe(Default("all.mainnet.ethdisco.net"))]
    pub address: String,
}

#[derive(Debug, FromStr)]
pub struct NR(pub NodeRecord);

#[derive(Debug, FromStr)]
pub struct Discv4NR(pub crate::sentry::devp2p::disc::v4::NodeRecord);

pub struct OptsDnsDisc {
    pub address: String,
}

impl OptsDnsDisc {
    pub fn make_task(self) -> anyhow::Result<DnsDiscovery> {
        info!("Starting DNS discovery fetch from {}", self.address);

        let dns_resolver = Resolver::new(Arc::new(
            TokioAsyncResolver::tokio_from_system_conf()
                .map_err(|err| format_err!("Failed to start DNS resolver: {err}"))?,
        ));

        let task = DnsDiscovery::new(Arc::new(dns_resolver), self.address, None);

        Ok(task)
    }
}

pub struct OptsDiscV4 {
    pub discv4_port: u16,
    pub discv4_bootnodes: Vec<Discv4NR>,
    pub discv4_cache: usize,
    pub discv4_concurrent_lookups: usize,
    pub listen_port: u16,
}

impl OptsDiscV4 {
    pub async fn make_task(self, secret_key: &SecretKey) -> anyhow::Result<Discv4> {
        info!("Starting discv4 at port {}", self.discv4_port);

        let bootstrap_nodes = self
            .discv4_bootnodes
            .into_iter()
            .map(|Discv4NR(nr)| nr)
            .collect::<Vec<_>>();

        let node = Node::new(
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

pub struct OptsDiscStatic {
    pub static_peers: Vec<NR>,
    pub static_peers_interval: u64,
}

impl OptsDiscStatic {
    pub fn make_task(self) -> anyhow::Result<StaticNodes> {
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
