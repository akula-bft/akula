//! RLPx protocol implementation in Rust

use super::{
    disc::Discovery,
    node_filter::{MemoryNodeFilter, NodeFilter},
    peer::*,
    transport::{TcpServer, TokioCidrListener, Transport},
    types::*,
};
use anyhow::{anyhow, bail, Context};
use cidr::IpCidr;
use educe::Educe;
use futures::sink::SinkExt;
use lru::LruCache;
use parking_lot::Mutex;
use secp256k1::SecretKey;
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    fmt::Debug,
    future::Future,
    net::SocketAddr,
    num::NonZeroUsize,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::{Duration, Instant},
};
use task_group::TaskGroup;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, unbounded_channel},
        oneshot::{channel as oneshot, Sender as OneshotSender},
        Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore,
    },
    time::sleep,
};
use tokio_stream::{StreamExt, StreamMap};
use tracing::*;
use uuid::Uuid;

const GRACE_PERIOD_SECS: u64 = 2;
const HANDSHAKE_TIMEOUT_SECS: u64 = 10;
const PING_TIMEOUT: Duration = Duration::from_secs(15);
const PING_INTERVAL: Duration = Duration::from_secs(60);
const DISCOVERY_TIMEOUT_SECS: u64 = 90;
const DISCOVERY_CONNECT_TIMEOUT_SECS: u64 = 5;

#[derive(Clone, Copy, Debug)]
enum DisconnectInitiator {
    Local,
    LocalForceful,
    Remote,
}

#[derive(Debug)]
struct DisconnectSignal {
    initiator: DisconnectInitiator,
    reason: DisconnectReason,
}

#[derive(Debug)]
struct ConnectedPeerState {
    _tasks: TaskGroup,
}

#[derive(Debug)]
enum PeerConnectionState {
    Connecting { connection_id: Uuid },
    Connected(ConnectedPeerState),
}

impl PeerConnectionState {
    const fn is_connected(&self) -> bool {
        matches!(self, Self::Connected(_))
    }
}

#[derive(Debug)]
struct PeerState {
    connection_state: PeerConnectionState,
    sem_permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
struct PeerStreams {
    /// Mapping of remote IDs to streams in `StreamMap`
    mapping: HashMap<PeerId, PeerState>,
    semaphore: Arc<Semaphore>,
}

impl PeerStreams {
    fn new(max_peers: usize) -> Self {
        Self {
            mapping: Default::default(),
            semaphore: Arc::new(Semaphore::new(max_peers)),
        }
    }

    fn disconnect_peer(&mut self, remote_id: PeerId) -> bool {
        self.mapping.remove(&remote_id).is_some()
    }
}

#[derive(Educe)]
#[educe(Clone)]
struct PeerStreamHandshakeData<C> {
    port: u16,
    secret_key: SecretKey,
    client_version: String,
    capabilities: Arc<CapabilitySet>,
    capability_server: Arc<C>,
}

async fn handle_incoming<TS, C>(
    task_group: Weak<TaskGroup>,
    streams: Arc<Mutex<PeerStreams>>,
    node_filter: Arc<Mutex<dyn NodeFilter>>,
    tcp_incoming: TS,
    handshake_data: PeerStreamHandshakeData<C>,
) where
    TS: TcpServer,
    C: CapabilityServer,
{
    let _: anyhow::Result<()> = async {
        loop {
            match tcp_incoming.accept().await {
                Err(e) => {
                    bail!("failed to accept peer: {:?}, shutting down", e);
                }
                Ok(stream) => {
                    let tasks = task_group
                        .upgrade()
                        .ok_or_else(|| anyhow!("task group is down"))?;

                    let task_name = format!("Incoming connection setup: {:?}", stream);

                    let f = handle_incoming_request(
                        streams.clone(),
                        node_filter.clone(),
                        stream,
                        handshake_data.clone(),
                    );
                    tasks.spawn_with_name(task_name, f);
                }
            }
        }
    }
    .await;
}

/// Set up newly connected peer's state, start its tasks
fn setup_peer_state<C, Io>(
    streams: Weak<Mutex<PeerStreams>>,
    capability_server: Arc<C>,
    remote_id: PeerId,
    peer: PeerStream<Io>,
) -> ConnectedPeerState
where
    C: CapabilityServer,
    Io: Transport,
{
    let capability_set = peer
        .capabilities()
        .iter()
        .copied()
        .map(|cap_info| (cap_info.name, cap_info.version))
        .collect::<HashMap<_, _>>();
    let (mut sink, mut stream) = futures::StreamExt::split(peer);
    let (peer_disconnect_tx, mut peer_disconnect_rx) = unbounded_channel();
    let tasks = TaskGroup::default();

    capability_server.on_peer_connect(remote_id, capability_set);

    let pinged = Arc::new(AtomicBool::default());
    let (pings_tx, mut pings) = channel(1);
    let (pongs_tx, mut pongs) = channel(1);

    // This will handle incoming packets from peer.
    tasks.spawn_with_name(format!("peer {} ingress router", remote_id), {
        let peer_disconnect_tx = peer_disconnect_tx;
        let capability_server = capability_server.clone();
        let pinged = pinged.clone();
        async move {
            let disconnect_signal = {
                async move {
                    while let Some(message) = stream.next().await {
                        match message {
                            Err(e) => {
                                debug!("Peer incoming error: {}", e);
                                break;
                            }
                            Ok(PeerMessage::Subprotocol(SubprotocolMessage {
                                cap_name,
                                message,
                            })) => {
                                // Actually handle the message
                                capability_server
                                    .on_peer_event(
                                        remote_id,
                                        InboundEvent::Message {
                                            capability_name: cap_name,
                                            message,
                                        },
                                    )
                                    .await
                            }
                            Ok(PeerMessage::Disconnect(reason)) => {
                                // Peer has requested disconnection.
                                return DisconnectSignal {
                                    initiator: DisconnectInitiator::Remote,
                                    reason,
                                };
                            }
                            Ok(PeerMessage::Ping) => {
                                let _ = pongs_tx.send(()).await;
                            }
                            Ok(PeerMessage::Pong) => {
                                // Pong received, peer is off the hook
                                pinged.store(false, Ordering::SeqCst);
                            }
                        }
                    }

                    // Ingress stream is closed, force disconnect the peer.
                    DisconnectSignal {
                        initiator: DisconnectInitiator::Remote,
                        reason: DisconnectReason::DisconnectRequested,
                    }
                }
            }
            .await;

            let _ = peer_disconnect_tx.send(disconnect_signal);
        }
        .instrument(span!(Level::DEBUG, "IN", "peer={}", remote_id.to_string(),))
    });

    // This will send our packets to peer.
    tasks.spawn_with_name(
        format!("peer {} egress router & disconnector", remote_id),
        async move {
            let mut event_fut = capability_server.next(remote_id);
            loop {
                let mut disconnecting = None;

                // Egress message and trigger to execute _after_ it is sent
                let mut egress = Option::<(PeerMessage, Option<OneshotSender<()>>)>::None;
                tokio::select! {
                    // Handle event from capability server.
                    msg = &mut event_fut => {
                        // Invariant: CapabilityServer::next() will never be called after disconnect event
                        match msg {
                            OutboundEvent::Message {
                                capability_name, message
                            } => {
                                event_fut = capability_server.next(remote_id);
                                egress = Some((PeerMessage::Subprotocol(SubprotocolMessage {
                                    cap_name: capability_name, message
                                }), None));
                            }
                            OutboundEvent::Disconnect {
                                reason
                            } => {
                                egress = Some((PeerMessage::Disconnect(reason), None));
                                disconnecting = Some(DisconnectSignal {
                                    initiator: DisconnectInitiator::Local, reason
                                });
                            }
                        };
                    },
                    // We ping the peer.
                    Some(tx) = pings.recv() => {
                        egress = Some((PeerMessage::Ping, Some(tx)));
                    }
                    // Peer has pinged us.
                    Some(_) = pongs.recv() => {
                        egress = Some((PeerMessage::Pong, None));
                    }
                    // Ping timeout or signal from ingress router.
                    Some(DisconnectSignal { initiator, reason }) = peer_disconnect_rx.recv() => {
                        if let DisconnectInitiator::Local = initiator {
                            egress = Some((PeerMessage::Disconnect(reason), None));
                        }
                        disconnecting = Some(DisconnectSignal { initiator, reason })
                    }
                };

                if let Some((message, trigger)) = egress {
                    trace!("Sending message: {:?}", message);

                    // Send egress message, force disconnect on error.
                    if let Err(e) = sink.send(message).await {
                        debug!("peer disconnected with error {:?}", e);
                        disconnecting.get_or_insert(DisconnectSignal {
                            initiator: DisconnectInitiator::LocalForceful,
                            reason: DisconnectReason::TcpSubsystemError,
                        });
                    } else if let Some(trigger) = trigger {
                        // Reason for signal in trigger:
                        // We don't want to timeout peer if our TCP socket is too slow
                        let _ = trigger.send(());
                    }
                }

                if let Some(DisconnectSignal { initiator, reason }) = disconnecting {
                    debug!("Disconnecting, initiated by {initiator:?} for reason {reason:?}");
                    if let DisconnectInitiator::Local = initiator {
                        // We have sent disconnect message, wait for grace period.
                        sleep(Duration::from_secs(GRACE_PERIOD_SECS)).await;
                    }
                    capability_server
                        .on_peer_event(
                            remote_id,
                            InboundEvent::Disconnect {
                                reason: Some(reason),
                            },
                        )
                        .await;
                    break;
                }
            }

            // We are done, drop the peer state.
            if let Some(streams) = streams.upgrade() {
                // This is the last line guaranteed to be executed.
                // After this the peer's task group is dropped and any alive tasks are forcibly cancelled.
                streams.lock().disconnect_peer(remote_id);
            }
        }
        .instrument(span!(
            Level::DEBUG,
            "OUT/DISC",
            "peer={}",
            remote_id.to_string(),
        )),
    );

    // This will ping the peer and disconnect if they don't respond.
    tasks.spawn_with_name(format!("peer {} pinger", remote_id), async move {
        loop {
            pinged.store(true, Ordering::SeqCst);

            let (cb_tx, cb_rx) = oneshot();

            // Pipes went down, pinger must exit
            let _ = pings_tx.send(cb_tx).await;
            let _ = cb_rx.await;

            sleep(PING_INTERVAL).await;
        }
    });
    ConnectedPeerState { _tasks: tasks }
}

/// Establishes the connection with peer and adds them to internal state.
async fn handle_incoming_request<C, Io>(
    streams: Arc<Mutex<PeerStreams>>,
    node_filter: Arc<Mutex<dyn NodeFilter>>,
    stream: Io,
    handshake_data: PeerStreamHandshakeData<C>,
) where
    C: CapabilityServer,
    Io: Transport,
{
    let PeerStreamHandshakeData {
        secret_key,
        client_version,
        capabilities,
        capability_server,
        port,
    } = handshake_data;
    // Do handshake and convert incoming connection into stream.
    let peer_res = tokio::time::timeout(
        Duration::from_secs(HANDSHAKE_TIMEOUT_SECS),
        PeerStream::incoming(
            stream,
            secret_key,
            client_version,
            capabilities.get_capabilities().to_vec(),
            port,
        ),
    )
    .await
    .unwrap_or_else(|_| Err(anyhow!("incoming connection timeout")));

    match peer_res {
        Ok(peer) => {
            let remote_id = peer.remote_id();
            let s = streams.clone();
            let mut s = s.lock();
            let node_filter = node_filter.clone();
            let PeerStreams { mapping, semaphore } = &mut *s;
            let total_connections = mapping.len();

            match mapping.entry(remote_id) {
                Entry::Occupied(entry) => {
                    debug!(
                        "We are already {} to remote peer {}!",
                        if entry.get().connection_state.is_connected() {
                            "connected"
                        } else {
                            "connecting"
                        },
                        remote_id
                    );
                }
                Entry::Vacant(entry) => {
                    if let Ok(sem_permit) = semaphore.clone().try_acquire_owned() {
                        if node_filter.lock().is_allowed(total_connections, remote_id) {
                            debug!("New incoming peer connected: {}", remote_id);
                            entry.insert(PeerState {
                                connection_state: PeerConnectionState::Connected(setup_peer_state(
                                    Arc::downgrade(&streams),
                                    capability_server,
                                    remote_id,
                                    peer,
                                )),
                                sem_permit,
                            });
                        } else {
                            trace!("Node filter rejected peer {}, disconnecting", remote_id);
                        }
                    }
                }
            }
        }
        Err(e) => {
            debug!("Peer disconnected with error {}", e);
        }
    }
}

#[derive(Debug, Default)]
struct CapabilitySet {
    capability_cache: Vec<CapabilityInfo>,
}

impl CapabilitySet {
    fn get_capabilities(&self) -> &[CapabilityInfo] {
        &self.capability_cache
    }
}

impl From<BTreeMap<CapabilityId, CapabilityLength>> for CapabilitySet {
    fn from(inner: BTreeMap<CapabilityId, CapabilityLength>) -> Self {
        let capability_cache = inner
            .iter()
            .map(
                |(&CapabilityId { name, version }, &length)| CapabilityInfo {
                    name,
                    version,
                    length,
                },
            )
            .collect();

        Self { capability_cache }
    }
}

/// This is an asynchronous RLPx server implementation.
///
/// `Swarm` is the representation of swarm of connected RLPx peers that
/// supports registration for capability servers.
///
/// This implementation is based on the concept of structured concurrency.
/// Internal state is managed by a multitude of workers that run in separate runtime tasks
/// spawned on the running executor during the server creation and addition of new peers.
/// All continuously running workers are inside the task scope owned by the server struct.
#[derive(Educe)]
#[educe(Debug)]
pub struct Swarm<C: CapabilityServer> {
    #[allow(unused)]
    tasks: Arc<TaskGroup>,

    streams: Arc<Mutex<PeerStreams>>,

    currently_connecting: Arc<AtomicUsize>,

    node_filter: Arc<Mutex<dyn NodeFilter>>,

    capabilities: Arc<CapabilitySet>,
    #[educe(Debug(ignore))]
    capability_server: Arc<C>,

    #[educe(Debug(ignore))]
    secret_key: SecretKey,
    client_version: String,
    port: u16,
}

/// Builder for ergonomically creating a new `Server`.
#[derive(Debug)]
pub struct SwarmBuilder {
    task_group: Option<Arc<TaskGroup>>,
    listen_options: Option<ListenOptions>,
    client_version: String,
}

impl SwarmBuilder {
    pub fn with_task_group(mut self, task_group: Arc<TaskGroup>) -> Self {
        self.task_group = Some(task_group);
        self
    }

    pub fn with_listen_options(mut self, options: ListenOptions) -> Self {
        self.listen_options = Some(options);
        self
    }

    pub fn with_client_version(mut self, version: String) -> Self {
        self.client_version = version;
        self
    }

    /// Create a new RLPx node
    pub async fn build<C: CapabilityServer>(
        self,
        capability_mask: BTreeMap<CapabilityId, CapabilityLength>,
        capability_server: Arc<C>,
        secret_key: SecretKey,
    ) -> anyhow::Result<Arc<Swarm<C>>> {
        Swarm::new_inner(
            secret_key,
            self.client_version,
            self.task_group,
            capability_mask.into(),
            capability_server,
            self.listen_options,
        )
        .await
    }
}

#[derive(Educe)]
#[educe(Debug)]
pub struct ListenOptions {
    #[educe(Debug(ignore))]
    discovery_tasks: Arc<AsyncMutex<StreamMap<String, Discovery>>>,
    min_peers: usize,
    max_peers: NonZeroUsize,
    addr: SocketAddr,
    cidr: Option<IpCidr>,
    no_new_peers: Arc<AtomicBool>,
}

impl ListenOptions {
    pub fn new(
        discovery_tasks: StreamMap<String, Discovery>,
        min_peers: usize,
        max_peers: NonZeroUsize,
        addr: SocketAddr,
        cidr: Option<IpCidr>,
        no_new_peers: Arc<AtomicBool>,
    ) -> Self {
        Self {
            discovery_tasks: Arc::new(AsyncMutex::new(discovery_tasks)),
            min_peers,
            max_peers,
            addr,
            cidr,
            no_new_peers,
        }
    }
}

impl Swarm<()> {
    pub fn builder() -> SwarmBuilder {
        SwarmBuilder {
            task_group: None,
            listen_options: None,
            client_version: format!("rust-devp2p/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

impl<C: CapabilityServer> Swarm<C> {
    pub async fn new(
        capability_mask: BTreeMap<CapabilityId, CapabilityLength>,
        capability_server: Arc<C>,
        secret_key: SecretKey,
    ) -> anyhow::Result<Arc<Self>> {
        Swarm::builder()
            .build(capability_mask, capability_server, secret_key)
            .await
    }

    #[allow(unreachable_code)]
    async fn new_inner(
        secret_key: SecretKey,
        client_version: String,
        task_group: Option<Arc<TaskGroup>>,
        capabilities: CapabilitySet,
        capability_server: Arc<C>,
        listen_options: Option<ListenOptions>,
    ) -> anyhow::Result<Arc<Self>> {
        let tasks = task_group.unwrap_or_default();

        let port = listen_options
            .as_ref()
            .map_or(0, |options| options.addr.port());

        let max_peers = listen_options
            .as_ref()
            .map_or(usize::MAX, |options| options.max_peers.get());
        let streams = Arc::new(Mutex::new(PeerStreams::new(max_peers)));
        let node_filter = Arc::new(Mutex::new(MemoryNodeFilter::new(Arc::new(
            max_peers.into(),
        ))));

        let capabilities = Arc::new(capabilities);

        if let Some(options) = &listen_options {
            let tcp_incoming = TcpListener::bind(options.addr)
                .await
                .context("Failed to bind RLPx node to socket")?;
            let cidr = options.cidr;
            tasks.spawn_with_name("incoming handler", {
                let handshake_data = PeerStreamHandshakeData {
                    port,
                    secret_key,
                    client_version: client_version.clone(),
                    capabilities: capabilities.clone(),
                    capability_server: capability_server.clone(),
                };

                handle_incoming(
                    Arc::downgrade(&tasks),
                    streams.clone(),
                    node_filter.clone(),
                    TokioCidrListener::new(tcp_incoming, cidr),
                    handshake_data,
                )
            });
        }

        let server = Arc::new(Self {
            tasks: tasks.clone(),
            streams,
            currently_connecting: Default::default(),
            node_filter,
            capabilities,
            capability_server,
            secret_key,
            client_version,
            port,
        });

        if let Some(options) = listen_options {
            tasks.spawn_with_name("dialer", {
                let server = Arc::downgrade(&server);
                let tasks = tasks.clone();
                async move {
                    let banlist = Arc::new(Mutex::new(LruCache::new(10_000)));

                    for worker in 0..options.max_peers.get() {
                        tasks.spawn_with_name(format!("dialer #{worker}"), {
                            let banlist = banlist.clone();
                            let server = server.clone();
                            let discovery_tasks = options.discovery_tasks.clone();
                            let no_new_peers = options.no_new_peers.clone();
                            async move {
                                loop {
                                    while let Some(num_peers) = server.upgrade().map(|server| server.num_peers()) {
                                        if !no_new_peers.load(Ordering::SeqCst) && (num_peers < options.min_peers || worker == 1) && num_peers < max_peers {
                                            let next_peer = discovery_tasks.lock().await.next().await;
                                            match next_peer {
                                                None => (),
                                                Some((disc_id, Err(e))) => {
                                                    debug!("Failed to get new peer: {e} ({disc_id})")
                                                }
                                                Some((disc_id, Ok(NodeRecord { id, addr }))) => {
                                                    let now = Instant::now();
                                                    if let Some(banned_timestamp) =
                                                        banlist.lock().get_mut(&id).copied()
                                                    {
                                                        let time_since_ban: Duration =
                                                            now - banned_timestamp;
                                                        if time_since_ban <= Duration::from_secs(300) {
                                                            let secs_since_ban = time_since_ban.as_secs();
                                                            debug!(
                                                                "Skipping failed peer ({id}, failed {secs_since_ban}s ago)",
                                                            );
                                                            continue;
                                                        }
                                                    }

                                                    if let Some(server) = server.upgrade() {
                                                        debug!("Dialing peer {id:?}@{addr} ({disc_id})");
                                                        if server
                                                            .add_peer_inner(addr, id, true)
                                                            .await
                                                            .is_err()
                                                        {
                                                            banlist.lock().put(id, Instant::now());
                                                        }
                                                    } else {
                                                        break;
                                                    }
                                                }
                                            }
                                        } else {
                                            let delay = 2000;
                                            debug!("Not accepting peers, delaying dial for {delay}ms");
                                            sleep(Duration::from_millis(delay)).await;
                                        }
                                    }
                                }

                                debug!("Quitting");
                            }
                            .instrument(span!(
                                Level::DEBUG,
                                "dialer",
                                worker
                            ))
                        });
                    }
                }
            });
        }

        Ok(server)
    }

    /// Add a new peer to this RLPx node. Returns `true` if it was added successfully (did not exist before, accepted by node filter).
    pub fn add_peer(
        &self,
        node_record: NodeRecord,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send + 'static {
        self.add_peer_inner(node_record.addr, node_record.id, false)
    }

    fn add_peer_inner(
        &self,
        addr: SocketAddr,
        remote_id: PeerId,
        untrusted_peer: bool,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send + 'static {
        let tasks = self.tasks.clone();
        let streams = self.streams.clone();
        let node_filter = self.node_filter.clone();

        let capability_set = self.capabilities.get_capabilities().to_vec();
        let capability_server = self.capability_server.clone();

        let secret_key = self.secret_key;
        let client_version = self.client_version.clone();
        let port = self.port;

        let (tx, rx) = tokio::sync::oneshot::channel();
        let connection_id = Uuid::new_v4();
        let currently_connecting = self.currently_connecting.clone();

        // Start reaper task that will terminate this connection if connection future gets dropped.
        tasks.spawn_with_name(format!("connection {} reaper", connection_id), {
            let cid = connection_id;
            let streams = streams.clone();
            let currently_connecting = currently_connecting.clone();
            async move {
                if rx.await.is_err() {
                    let mut s = streams.lock();
                    if let Entry::Occupied(entry) = s.mapping.entry(remote_id) {
                        // If this is the same connection attempt, then remove.
                        if let PeerConnectionState::Connecting { connection_id } =
                            entry.get().connection_state
                        {
                            if connection_id == cid {
                                trace!("Reaping failed outbound connection: {}/{}", remote_id, cid);

                                entry.remove();
                            }
                        }
                    }
                }
                currently_connecting.fetch_sub(1, Ordering::SeqCst);
            }
        });

        async move {
            let mut inserted = false;

            {
                let semaphore = streams.lock().semaphore.clone();
                trace!("Awaiting semaphore permit");
                let sem_permit = match semaphore.acquire_owned().await {
                    Ok(v) => v,
                    Err(_) => return Ok(false),
                };
                trace!("Semaphore permit acquired");

                currently_connecting.fetch_add(1, Ordering::SeqCst);

                let mut streams = streams.lock();
                let node_filter = node_filter.lock();

                let connection_num = streams.mapping.len();

                match streams.mapping.entry(remote_id) {
                    Entry::Occupied(key) => {
                        debug!(
                            "We are already {} to remote peer {}!",
                            if key.get().connection_state.is_connected() {
                                "connected"
                            } else {
                                "connecting"
                            },
                            remote_id
                        );
                    }
                    Entry::Vacant(vacant) => {
                        if untrusted_peer && !node_filter.is_allowed(connection_num, remote_id) {
                            trace!("rejecting peer {}", remote_id);
                        } else {
                            debug!("connecting to peer {} at {}", remote_id, addr);

                            vacant.insert(PeerState {
                                connection_state: PeerConnectionState::Connecting { connection_id },
                                sem_permit,
                            });
                            inserted = true;
                        }
                    }
                }
            }

            if !inserted {
                return Ok(false);
            }

            // Connecting to peer is a long running operation so we have to break the mutex lock.
            let peer_res = async {
                let transport = TcpStream::connect(addr).await?;
                PeerStream::connect(
                    transport,
                    secret_key,
                    remote_id,
                    client_version,
                    capability_set,
                    port,
                )
                .await
            }
            .await;

            let streams = streams.clone();
            let mut streams_guard = streams.lock();
            let PeerStreams { mapping, .. } = &mut *streams_guard;

            // Adopt the new connection if the peer has not been dropped or superseded by incoming connection.
            if let Entry::Occupied(mut peer_state) = mapping.entry(remote_id) {
                if !peer_state.get().connection_state.is_connected() {
                    match peer_res {
                        Ok(peer) => {
                            assert_eq!(peer.remote_id(), remote_id);
                            debug!("New peer connected: {}", remote_id);

                            peer_state.get_mut().connection_state =
                                PeerConnectionState::Connected(setup_peer_state(
                                    Arc::downgrade(&streams),
                                    capability_server,
                                    remote_id,
                                    peer,
                                ));

                            let _ = tx.send(());
                            return Ok(true);
                        }
                        Err(e) => {
                            debug!("Peer {:?} disconnected with error: {}", remote_id, e);
                            peer_state.remove();
                            return Err(e);
                        }
                    }
                }
            }

            Ok(false)
        }
        .instrument(span!(
            Level::DEBUG,
            "add peer",
            "remote_id={}",
            &*remote_id.to_string()
        ))
    }

    /// Returns the number of peers we're currently dialing
    pub fn dialing(&self) -> usize {
        self.currently_connecting.load(Ordering::SeqCst)
    }

    /// All peers
    pub fn num_peers(&self) -> usize {
        self.streams.lock().mapping.len()
    }
}

impl<C: CapabilityServer> Deref for Swarm<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &*self.capability_server
    }
}
