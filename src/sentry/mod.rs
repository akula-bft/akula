#![allow(dead_code, clippy::upper_case_acronyms)]

use self::eth::*;
use crate::{
    binutil::AkulaDataDir, models::P2PParams, sentry::services::SentryService, version_string,
};
use anyhow::{format_err, Context};
use async_stream::stream;
use async_trait::async_trait;
use cidr::IpCidr;
use clap::Parser;
use derive_more::FromStr;
use devp2p::*;
use disc::dns::Resolver;
use educe::Educe;
use ethereum_interfaces::sentry::{self, sentry_server::SentryServer, InboundMessage, PeerEvent};
use fastrlp::Decodable;
use futures::stream::BoxStream;
use maplit::btreemap;
use num_traits::{FromPrimitive, ToPrimitive};
use parking_lot::RwLock;
use secp256k1::{PublicKey, SecretKey, SECP256K1};
use std::{
    self,
    collections::{btree_map::Entry, hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet},
    fmt::Debug,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use task_group::TaskGroup;
use tokio::sync::{
    broadcast::{channel as broadcast_channel, Sender as BroadcastSender},
    mpsc::{channel, Sender},
    Mutex as AsyncMutex,
};
use tokio_stream::StreamExt;
use tonic::transport::Server;
use tracing::*;
use trust_dns_resolver::TokioAsyncResolver;

pub mod devp2p;
pub mod eth;
pub mod grpc;
pub mod services;

type OutboundSender = Sender<OutboundEvent>;
type OutboundReceiver = Arc<AsyncMutex<BoxStream<'static, OutboundEvent>>>;

pub const BUFFERING_FACTOR: usize = 5;

/// INITIAL_WINDOW_SIZE upper bound
pub const MAX_INITIAL_WINDOW_SIZE: u32 = (1 << 31) - 1;

/// MAX_FRAME_SIZE upper bound
pub const MAX_FRAME_SIZE: u32 = (1 << 24) - 1;
const THROTTLE_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, FromStr)]
pub struct NR(pub NodeRecord);

#[derive(Clone, Debug, FromStr)]
pub struct Discv4NR(pub crate::sentry::devp2p::disc::v4::NodeRecord);

#[derive(Clone)]
pub struct Pipes {
    sender: OutboundSender,
    receiver: OutboundReceiver,
}

#[derive(Clone, Debug, Default)]
struct BlockTracker {
    block_by_peer: HashMap<PeerId, u64>,
    peers_by_block: BTreeMap<u64, HashSet<PeerId>>,
}

impl BlockTracker {
    fn set_block_number(&mut self, peer: PeerId, block: u64, force_create: bool) {
        match self.block_by_peer.entry(peer) {
            HashMapEntry::Vacant(e) => {
                if force_create {
                    e.insert(block);
                } else {
                    return;
                }
            }
            HashMapEntry::Occupied(mut e) => {
                if *e.get() > block {
                    return;
                }
                let old_block = std::mem::replace(e.get_mut(), block);
                if let Entry::Occupied(mut entry) = self.peers_by_block.entry(old_block) {
                    entry.get_mut().remove(&peer);

                    if entry.get().is_empty() {
                        entry.remove();
                    }
                }
            }
        }

        self.peers_by_block.entry(block).or_default().insert(peer);
    }

    fn remove_peer(&mut self, peer: PeerId) {
        if let Some(block) = self.block_by_peer.remove(&peer) {
            if let Entry::Occupied(mut entry) = self.peers_by_block.entry(block) {
                entry.get_mut().remove(&peer);

                if entry.get().is_empty() {
                    entry.remove();
                }
            }
        }
    }

    fn peers_with_min_block(&self, block: u64) -> HashSet<PeerId> {
        self.peers_by_block
            .range(block..)
            .flat_map(|(_, v)| v)
            .copied()
            .collect()
    }
}

#[derive(Educe)]
#[educe(Debug)]
pub struct CapabilityServerImpl {
    #[educe(Debug(ignore))]
    pub peer_pipes: Arc<RwLock<HashMap<PeerId, Pipes>>>,
    block_tracker: Arc<RwLock<BlockTracker>>,

    status_message: Arc<RwLock<Option<FullStatusData>>>,
    protocol_version: EthProtocolVersion,
    valid_peers: Arc<RwLock<HashSet<PeerId>>>,

    data_sender: BroadcastSender<InboundMessage>,
    peers_status_sender: BroadcastSender<PeerEvent>,

    no_new_peers: Arc<AtomicBool>,
}

impl CapabilityServerImpl {
    pub fn new(protocol_version: EthProtocolVersion, max_peers: NonZeroUsize) -> Self {
        Self {
            peer_pipes: Default::default(),
            block_tracker: Default::default(),
            status_message: Default::default(),
            protocol_version,
            valid_peers: Default::default(),
            data_sender: broadcast_channel(max_peers.get() * BUFFERING_FACTOR).0,
            peers_status_sender: broadcast_channel(max_peers.get()).0,
            no_new_peers: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn no_new_peers_handle(&self) -> Arc<AtomicBool> {
        self.no_new_peers.clone()
    }

    fn setup_peer(&self, peer: PeerId, p: Pipes) {
        let mut pipes = self.peer_pipes.write();
        let mut block_tracker = self.block_tracker.write();

        assert!(pipes.insert(peer, p).is_none());
        block_tracker.set_block_number(peer, 0, true);
    }

    fn get_pipes(&self, peer: PeerId) -> Option<Pipes> {
        self.peer_pipes.read().get(&peer).cloned()
    }

    pub fn sender(&self, peer: PeerId) -> Option<OutboundSender> {
        self.peer_pipes
            .read()
            .get(&peer)
            .map(|pipes| pipes.sender.clone())
    }

    fn receiver(&self, peer: PeerId) -> Option<OutboundReceiver> {
        self.peer_pipes
            .read()
            .get(&peer)
            .map(|pipes| pipes.receiver.clone())
    }

    #[instrument(name = "CapabilityServerImpl.teardown_peer", skip(self))]
    fn teardown_peer(&self, peer: PeerId) {
        let mut pipes = self.peer_pipes.write();
        let mut block_tracker = self.block_tracker.write();
        let mut valid_peers = self.valid_peers.write();

        pipes.remove(&peer);
        block_tracker.remove_peer(peer);
        valid_peers.remove(&peer);

        let send_status_result =
            self.peers_status_sender
                .send(ethereum_interfaces::sentry::PeerEvent {
                    peer_id: Some(ethereum_interfaces::types::H512::from(peer)),
                    event_id: ethereum_interfaces::sentry::peer_event::PeerEventId::Disconnect
                        as i32,
                });
        if send_status_result.is_err() {
            debug!("No subscribers to report peer status to");
        }
    }

    pub fn all_peers(&self) -> HashSet<PeerId> {
        self.peer_pipes.read().keys().copied().collect()
    }

    pub fn connected_peers(&self) -> usize {
        self.valid_peers.read().len()
    }

    pub fn set_status(&self, message: FullStatusData) {
        *self.status_message.write() = Some(message);
        self.no_new_peers.store(false, Ordering::SeqCst);
    }

    #[instrument(name = "CapabilityServerImpl.handle_event", skip(self, event))]
    fn handle_event(&self, peer: PeerId, event: InboundEvent) -> Result<(), DisconnectReason> {
        match event {
            InboundEvent::Disconnect { reason } => {
                debug!("Peer disconnect (reason: {:?}), tearing down peer.", reason);
                self.teardown_peer(peer);
            }
            InboundEvent::Message {
                message: Message { id, data },
                ..
            } => {
                let valid_peer = self.valid_peers.read().contains(&peer);
                let message_id = EthMessageId::from_usize(id);
                match message_id {
                    None => {
                        debug!("Unknown message");
                    }
                    Some(EthMessageId::Status) => {
                        let v = StatusMessage::decode(&mut &*data).map_err(|e| {
                            debug!("Failed to decode status message: {}! Kicking peer.", e);

                            DisconnectReason::ProtocolBreach
                        })?;

                        debug!("Decoded status message: {:?}", v);

                        let status_data = &*(self.status_message.read());
                        if let Some(FullStatusData { fork_filter, .. }) = status_data {
                            fork_filter.validate(v.fork_id).map_err(|reason| {
                                debug!("Kicking peer with incompatible fork ID: {:?}", reason);

                                DisconnectReason::UselessPeer
                            })?;

                            self.valid_peers.write().insert(peer);

                            let _ = self
                                .peers_status_sender
                                .send(ethereum_interfaces::sentry::PeerEvent {
                                peer_id: Some(ethereum_interfaces::types::H512::from(peer)),
                                event_id:
                                    ethereum_interfaces::sentry::peer_event::PeerEventId::Connect
                                        as i32,
                            });
                        }
                    }
                    Some(inbound_id) if valid_peer => {
                        let _ = self.data_sender.send(InboundMessage {
                            id: sentry::MessageId::from(inbound_id) as i32,
                            data,
                            peer_id: Some(peer.into()),
                        });
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl CapabilityServer for CapabilityServerImpl {
    #[instrument(skip(self, peer), level = "debug", fields(peer=&*peer.to_string()))]
    fn on_peer_connect(&self, peer: PeerId, caps: HashMap<CapabilityName, CapabilityVersion>) {
        let first_events = if let Some(FullStatusData {
            status,
            fork_filter,
        }) = &*self.status_message.read()
        {
            let status_message = StatusMessage {
                protocol_version: *caps
                    .get(&capability_name())
                    .expect("peer without this cap would have been disconnected"),
                network_id: status.network_id,
                total_difficulty: status.total_difficulty,
                best_hash: status.best_hash,
                genesis_hash: status.fork_data.genesis,
                fork_id: fork_filter.current(),
            };

            vec![OutboundEvent::Message {
                capability_name: capability_name(),
                message: Message {
                    id: EthMessageId::Status.to_usize().unwrap(),
                    data: fastrlp::encode_fixed_size(&status_message).to_vec().into(),
                },
            }]
        } else {
            vec![OutboundEvent::Disconnect {
                reason: DisconnectReason::DisconnectRequested,
            }]
        };

        let (sender, mut receiver) = channel(1);
        self.setup_peer(
            peer,
            Pipes {
                sender,
                receiver: Arc::new(AsyncMutex::new(Box::pin(stream! {
                    for event in first_events {
                        yield event;
                    }

                    while let Some(event) = receiver.recv().await {
                        yield event;
                    }
                }))),
            },
        );
    }

    #[instrument(skip_all, level = "debug", fields(peer=&*peer.to_string(), event=&*event.to_string()))]
    async fn on_peer_event(&self, peer: PeerId, event: InboundEvent) {
        debug!("Received message");

        if let Err(reason) = self.handle_event(peer, event) {
            match self.sender(peer) {
                Some(sender) => {
                    let _ = sender.send(OutboundEvent::Disconnect { reason }).await;
                }
                None => {
                    self.teardown_peer(peer);
                }
            }
        }
    }
    async fn next(&self, peer: PeerId) -> OutboundEvent {
        self.receiver(peer)
            .unwrap()
            .lock()
            .await
            .next()
            .await
            .unwrap_or(OutboundEvent::Disconnect {
                reason: DisconnectReason::DisconnectRequested,
            })
    }
}

#[derive(Educe, Parser)]
#[educe(Debug)]
pub struct Opts {
    #[clap(long)]
    #[educe(Debug(ignore))]
    pub node_key: Option<String>,
    #[clap(long, default_value = "0.0.0.0")]
    pub listen_addr: IpAddr,
    #[clap(long, default_value = "30303")]
    pub listen_port: u16,
    #[clap(long)]
    pub cidr: Option<IpCidr>,
    #[clap(long, default_value = "127.0.0.1:8000")]
    pub sentry_addr: SocketAddr,
    #[clap(long)]
    pub dnsdisc_address: Option<String>,
    #[clap(long, default_value = "30303")]
    pub discv4_port: u16,
    #[clap(long)]
    pub discv4_bootnodes: Vec<Discv4NR>,
    #[clap(long, default_value = "1000")]
    pub discv4_cache: usize,
    #[clap(long, default_value = "25")]
    pub discv4_concurrent_lookups: usize,
    #[clap(long)]
    pub static_peers: Vec<NR>,
    #[clap(long, default_value = "5000")]
    pub static_peers_interval: u64,
    #[clap(long, default_value = "100")]
    pub max_peers: NonZeroUsize,
    /// Minimum number of peers, below which we will search for peers more aggressively.
    #[clap(long, default_value = "10")]
    pub min_peers: usize,
    /// Disable DNS and UDP discovery, only use static peers.
    #[clap(long, num_args = 0)]
    pub no_discovery: bool,
    /// Disable DNS discovery
    #[clap(long, num_args = 0)]
    pub no_dns_discovery: bool,
}

pub async fn run(
    opts: Opts,
    db_path: AkulaDataDir,
    network_params: P2PParams,
) -> anyhow::Result<Arc<Swarm<CapabilityServerImpl>>> {
    let secret_key = {
        let secret_key_path = db_path.nodekey();
        let secret_key;
        if let Some(node_key) = opts.node_key {
            secret_key = SecretKey::from_slice(&hex::decode(node_key)?)?;
            info!("Loaded node key from config");
            std::fs::write(&secret_key_path, &hex::encode(secret_key.secret_bytes()))?;
        } else {
            match std::fs::read_to_string(&secret_key_path) {
                Ok(nodekey) => {
                    secret_key = SecretKey::from_slice(&hex::decode(&nodekey)?)?;
                    info!(
                        "Loaded node key: {}",
                        hex::encode(secret_key.secret_bytes())
                    );
                }
                Err(e) => match e.kind() {
                    std::io::ErrorKind::NotFound => {
                        secret_key = SecretKey::new(&mut secp256k1::rand::thread_rng());
                        info!(
                            "Generated new node key: {}",
                            hex::encode(secret_key.secret_bytes())
                        );
                        std::fs::write(&secret_key_path, &hex::encode(secret_key.secret_bytes()))?;
                    }
                    _ => return Err(e.into()),
                },
            }
        }
        secret_key
    };

    let listen_addr = SocketAddr::new(opts.listen_addr, opts.listen_port);

    info!("Starting Ethereum P2P node");

    info!(
        "Node ID: {}",
        hex::encode(
            devp2p::util::pk2id(&PublicKey::from_secret_key(SECP256K1, &secret_key)).as_bytes()
        )
    );

    if let Some(cidr_filter) = &opts.cidr {
        info!("Peers restricted to range {}", cidr_filter);
    }

    let mut discovery_tasks: HashMap<String, Discovery> = HashMap::new();

    let bootnodes = if opts.discv4_bootnodes.is_empty() {
        network_params
            .bootnodes
            .iter()
            .map(|b| Discv4NR(b.parse().unwrap()))
            .collect::<Vec<_>>()
    } else {
        opts.discv4_bootnodes
    };

    let dns_addr = opts.dnsdisc_address.or(network_params.dns);

    let discv4_throttle = Arc::new(AtomicBool::new(false));

    if !opts.no_discovery {
        if !opts.no_dns_discovery {
            if let Some(dns_addr) = dns_addr {
                info!("Starting DNS discovery fetch from {}", dns_addr);

                let dns_resolver = Resolver::new(Arc::new(
                    TokioAsyncResolver::tokio_from_system_conf()
                        .map_err(|err| format_err!("Failed to start DNS resolver: {err}"))?,
                ));

                let task = DnsDiscovery::new(Arc::new(dns_resolver), dns_addr, None);

                discovery_tasks.insert("dnsdisc".to_string(), Box::pin(task));
            }
        }

        info!("Starting discv4 at port {}", opts.discv4_port);

        let bootstrap_nodes = bootnodes
            .into_iter()
            .map(|Discv4NR(nr)| nr)
            .collect::<Vec<_>>();

        let node = disc::v4::Node::new(
            format!("0.0.0.0:{}", opts.discv4_port).parse().unwrap(),
            secret_key,
            bootstrap_nodes,
            None,
            true,
            opts.listen_port,
        )
        .await?;

        let task = Discv4Builder::default()
            .with_cache(opts.discv4_cache)
            .with_concurrent_lookups(opts.discv4_concurrent_lookups)
            .with_throttle(discv4_throttle.clone())
            .build(node);

        discovery_tasks.insert("discv4".to_string(), Box::pin(task));
    }

    if !opts.static_peers.is_empty() {
        info!("Enabling static peers: {:?}", opts.static_peers);

        let task = StaticNodes::new(
            opts.static_peers
                .iter()
                .map(|&NR(NodeRecord { addr, id })| (addr, id))
                .collect::<HashMap<_, _>>(),
            Duration::from_millis(opts.static_peers_interval),
        );

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
            opts.min_peers,
            opts.max_peers,
            listen_addr,
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

    if !opts.no_discovery {
        let swarm = swarm.clone();
        tasks.spawn_with_name("discv4 throttler", async move {
            loop {
                discv4_throttle.store(swarm.num_peers() >= opts.min_peers, Ordering::SeqCst);
                tokio::time::sleep(THROTTLE_INTERVAL).await;
            }
        });
    }

    info!("RLPx node listening at {}", listen_addr);

    tasks.spawn(async move {
        let svc = SentryServer::new(SentryService::new(capability_server));

        info!("Sentry gRPC server starting on {}", opts.sentry_addr);

        Server::builder()
            .add_service(svc)
            .serve(opts.sentry_addr)
            .await
            .unwrap();
    });

    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(swarm)
}
