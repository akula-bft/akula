#![allow(dead_code, clippy::upper_case_acronyms)]
use akula::akula_tracing::{self, Component};
use clap::Parser;
use educe::Educe;
use std::time::Duration;
use tokio::time::sleep;
use tracing::*;
use tracing_subscriber::prelude::*;

#[derive(Educe, Parser)]
#[clap(
    name = "ethereum-sentry",
    about = "Service that listens to Ethereum's P2P network, serves information to other nodes, and provides gRPC interface to clients to interact with the network."
)]
#[educe(Debug)]
pub struct Opts {
    #[clap(flatten)]
    pub sentry_opts: akula::sentry::Opts,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts: Opts = Opts::parse();
    fdlimit::raise_fd_limit();

    akula_tracing::build_subscriber(Component::Sentry).init();

    let max_peers = opts.sentry_opts.max_peers;
    let swarm = akula::sentry::run(opts.sentry_opts).await?;

    loop {
        info!(
            "Peer info: {} active (+{} dialing) / {} max.",
            swarm.connected_peers(),
            swarm.dialing(),
            max_peers
        );

        sleep(Duration::from_secs(5)).await;
    }
}
