use super::{kad::*, message::*, proto::*, util::*, NodeId};
use anyhow::{anyhow, bail, Context};
use bytes::{BufMut, BytesMut};
use chrono::Utc;
use fastrlp::*;
use futures_util::future::join_all;
use igd::aio::search_gateway;
use num_traits::FromPrimitive;
use parking_lot::{Mutex, RwLock};
use primitive_types::H256;
use rand::{distributions::Standard, prelude::SliceRandom, thread_rng, Rng};
use secp256k1::{
    ecdsa::{RecoverableSignature, RecoveryId},
    Message, PublicKey, SecretKey, SECP256K1,
};
use sha3::{Digest, Keccak256};
use std::{
    collections::{btree_map, hash_map, BTreeMap, HashMap},
    convert::TryFrom,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use task_group::TaskGroup;
use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::{channel, Sender},
        oneshot::{channel as oneshot, Sender as OneshotSender},
    },
    time::{sleep, timeout},
};
use tracing::*;
use url::{Host, Url};

pub type RequestId = u64;

pub const MAX_PACKET_SIZE: usize = 1280;

pub const UPNP_INTERVAL: Duration = Duration::from_secs(60);
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);
pub const REFRESH_TIMEOUT: Duration = Duration::from_secs(60);
pub const PING_INTERVAL: Duration = Duration::from_secs(10);
pub const FIND_NODE_TIMEOUT: Duration = Duration::from_secs(10);
pub const QUERY_AWAIT_PING_TIME: Duration = Duration::from_secs(2);
pub const NEIGHBOURS_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

fn expiry(timeout: Duration) -> u64 {
    u64::try_from(Utc::now().timestamp()).expect("this would predate the protocol inception")
        + timeout.as_secs()
}

fn ping_expiry() -> u64 {
    expiry(PING_TIMEOUT)
}

fn find_node_expiry() -> u64 {
    expiry(FIND_NODE_TIMEOUT)
}

pub const ALPHA: usize = 3;

#[derive(Clone, Copy, Debug, RlpEncodable, RlpDecodable)]
pub struct NodeRecord {
    pub address: Ip,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub id: NodeId,
}

#[derive(Debug, Error)]
pub enum NodeRecordParseError {
    #[error("failed to parse url")]
    InvalidUrl(#[source] anyhow::Error),
    #[error("failed to parse id")]
    InvalidId(#[source] anyhow::Error),
}

impl NodeRecord {
    /// The TCP socket address of this node
    #[must_use]
    pub fn tcp_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address.0, self.tcp_port)
    }

    /// The UDP socket address of this node
    #[must_use]
    pub fn udp_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address.0, self.udp_port)
    }
}

impl FromStr for NodeRecord {
    type Err = NodeRecordParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let url = Url::parse(s).map_err(|e| NodeRecordParseError::InvalidUrl(e.into()))?;

        let address = match url.host() {
            Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
            Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
            Some(Host::Domain(ip)) => {
                IpAddr::from_str(ip).map_err(|e| NodeRecordParseError::InvalidUrl(e.into()))?
            }
            other => {
                return Err(NodeRecordParseError::InvalidUrl(anyhow!(
                    "invalid host: {:?}",
                    other
                )))
            }
        }
        .into();
        let port = url
            .port()
            .ok_or_else(|| NodeRecordParseError::InvalidUrl(anyhow!("no port specified")))?;
        let id = url
            .username()
            .parse()
            .map_err(|e| NodeRecordParseError::InvalidId(anyhow::Error::from(e)))?;

        Ok(Self {
            address,
            id,
            tcp_port: port,
            udp_port: port,
        })
    }
}

type InflightFindNodeInner = HashMap<NodeId, HashMap<RequestId, Sender<NeighboursMessage>>>;

#[derive(Default)]
struct InflightFindNode {
    inner: Arc<Mutex<InflightFindNodeInner>>,
}

impl InflightFindNode {
    fn add(
        &self,
        node_id: NodeId,
        sender: Sender<NeighboursMessage>,
    ) -> InflightFindNodeRequestGuard {
        loop {
            let mut inner = self.inner.lock();
            let node_data = inner.entry(node_id).or_default();
            let request_id = rand::random();
            if let hash_map::Entry::Vacant(entry) = node_data.entry(request_id) {
                entry.insert(sender);
                trace!("Inserted sender {}/{}", node_id, request_id);
                return InflightFindNodeRequestGuard {
                    inner: self.inner.clone(),
                    node_id,
                    request_id,
                };
            }
        }
    }

    fn get(&self, node_id: NodeId) -> Vec<Sender<NeighboursMessage>> {
        if let Some(cbs) = self.inner.lock().get(&node_id) {
            return cbs.values().cloned().collect();
        }

        vec![]
    }
}

struct InflightFindNodeRequestGuard {
    inner: Arc<Mutex<InflightFindNodeInner>>,
    node_id: NodeId,
    request_id: RequestId,
}

impl Drop for InflightFindNodeRequestGuard {
    fn drop(&mut self) {
        let mut inner = self.inner.lock();
        let entry = inner.entry(self.node_id);
        if let hash_map::Entry::Occupied(mut entry) = entry {
            trace!("Dropping entry {}/{}", self.node_id, self.request_id);
            assert!(entry.get_mut().remove(&self.request_id).is_some());
            if entry.get().is_empty() {
                trace!(
                    "No more data for node {}, dropping node entry",
                    self.node_id
                );
                entry.remove();
            }
        } else {
            unreachable!("not anyone else's business to drop data")
        }
    }
}

pub struct Node {
    task_group: Arc<TaskGroup>,
    connected: Arc<Mutex<Table>>,

    id: NodeId,
    node_endpoint: Arc<RwLock<Endpoint>>,

    egress_requests_tx: Sender<(SocketAddr, NodeId, EgressMessage)>,
    expected_pings: Arc<Mutex<HashMap<SocketAddr, HashMap<RequestId, OneshotSender<()>>>>>,
    inflight_find_node_requests: Arc<InflightFindNode>,
}

enum PreTrigger {
    Ping(Option<OneshotSender<()>>),
}

enum PostSendTrigger {
    Ping,
}

impl Node {
    pub async fn new(
        addr: SocketAddr,
        secret_key: SecretKey,
        bootstrap_nodes: Vec<NodeRecord>,
        public_address: Option<IpAddr>,
        enable_upnp: bool,
        tcp_port: u16,
    ) -> anyhow::Result<Arc<Self>> {
        let node_endpoint = Arc::new(RwLock::new(Endpoint {
            address: Ip(public_address.unwrap_or_else(|| addr.ip())),
            udp_port: addr.port(),
            tcp_port,
        }));

        let task_group = Arc::new(TaskGroup::new());

        if enable_upnp {
            task_group.spawn_with_name("discv4 - UPnP", {
                let node_endpoint = node_endpoint.clone();
                async move {
                    loop {
                        match async {
                            Ok::<_, anyhow::Error>(
                                search_gateway(Default::default())
                                    .await?
                                    .get_external_ip()
                                    .await?,
                            )
                        }
                        .await
                        {
                            Ok(v) => {
                                debug!("Discovered public IP: {}", v);
                                node_endpoint.write().address = Ip(v);
                            }
                            Err(e) => {
                                debug!("Failed to get public IP: {}", e);
                            }
                        }
                        sleep(UPNP_INTERVAL).await;
                    }
                }
                .instrument(span!(Level::TRACE, "UPNP",))
            });
        }

        let id = pk2id(&PublicKey::from_secret_key(SECP256K1, &secret_key));

        debug!("Starting node with id: {}", id);

        let udp = Arc::new(UdpSocket::bind(&addr).await?);

        let (egress_requests_tx, mut egress_requests) = channel(1);

        let mut table = Table::new(id);
        for node in bootstrap_nodes {
            debug!("Adding bootstrap node: {:?}", node);
            table.add_verified(node);
        }

        let connected = Arc::new(Mutex::new(table));

        let inflight_find_node_requests = Arc::new(InflightFindNode::default());
        let inflight_ping_requests = Arc::new(Mutex::new(HashMap::<H256, Vec<_>>::default()));
        let expected_pings = Arc::new(Mutex::new(HashMap::<
            SocketAddr,
            HashMap<RequestId, OneshotSender<_>>,
        >::new()));

        task_group.spawn_with_name("discv4 egress router", {
            let task_group = Arc::downgrade(&task_group);
            let connected = connected.clone();
            let inflight_ping_requests = inflight_ping_requests.clone();
            let udp = udp.clone();
            async move {
                while let Some((addr, peer, message)) = egress_requests.recv().await {
                    async {
                        if peer == id {
                            return;
                        }

                        trace!("Sending datagram {:?}", message);

                        let mut pre_trigger = None;
                        let mut post_trigger = None;

                        let mut datagram = BytesMut::with_capacity(MAX_PACKET_SIZE);
                        let mut sig_bytes = datagram.split_off(H256::len_bytes());
                        let mut payload =
                            sig_bytes.split_off(secp256k1::constants::COMPACT_SIGNATURE_SIZE + 1);
                        match message {
                            EgressMessage::Ping(message, sender) => {
                                pre_trigger = Some(PreTrigger::Ping(sender));
                                post_trigger = Some(PostSendTrigger::Ping);
                                payload.put_u8(1);
                                message.encode(&mut payload);
                            }
                            EgressMessage::Pong(message) => {
                                payload.put_u8(2);
                                message.encode(&mut payload);
                            }
                            EgressMessage::FindNode(message) => {
                                payload.put_u8(3);
                                message.encode(&mut payload);
                            }
                            EgressMessage::Neighbours(message) => {
                                payload.put_u8(4);
                                message.encode(&mut payload);
                            }
                        }

                        let signature: RecoverableSignature = SECP256K1.sign_ecdsa_recoverable(
                            &Message::from_slice(&Keccak256::digest(&payload)).unwrap(),
                            &secret_key,
                        );

                        let (rec, sig) = signature.serialize_compact();
                        sig_bytes.extend_from_slice(&sig);
                        sig_bytes.put_u8(rec.to_i32() as u8);

                        sig_bytes.unsplit(payload);

                        let hash = keccak256(&sig_bytes);

                        datagram.extend_from_slice(hash.as_bytes());

                        datagram.unsplit(sig_bytes);

                        let mut do_send = false;
                        match pre_trigger {
                            Some(PreTrigger::Ping(sender)) => {
                                let mut inflight_ping_requests = inflight_ping_requests.lock();
                                let cbs = inflight_ping_requests.entry(hash).or_insert_with(|| {
                                    do_send = true;
                                    Vec::new()
                                });
                                if let Some(sender) = sender {
                                    cbs.push(sender);
                                }
                            }
                            None => {
                                do_send = true;
                            }
                        }

                        if !do_send {
                            return;
                        }

                        if let Err(e) = udp.send_to(&datagram, addr).await {
                            warn!("UDP socket send failure: {}", e);
                        } else if let Some(trigger) = post_trigger {
                            match trigger {
                                PostSendTrigger::Ping => {
                                    if let Some(task_group) = task_group.upgrade() {
                                        task_group.spawn({
                                            let connected = connected.clone();
                                            let inflight_ping_requests =
                                                inflight_ping_requests.clone();
                                            async move {
                                                sleep(PING_TIMEOUT).await;
                                                let mut connected = connected.lock();
                                                let mut inflight_ping_requests =
                                                    inflight_ping_requests.lock();
                                                if inflight_ping_requests.remove(&hash).is_some() {
                                                    connected.remove(peer);
                                                }
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                    .instrument(span!(
                        Level::TRACE,
                        "OUT",
                        "addr={},node={}",
                        addr,
                        &*peer.to_string(),
                    ))
                    .await;
                }
            }
        });

        task_group.spawn_with_name("discv4 ingress router", {
            let egress_requests_tx = egress_requests_tx.clone();
            let connected = connected.clone();
            let node_endpoint = node_endpoint.clone();
            let expected_pings = expected_pings.clone();
            let inflight_find_node_requests = inflight_find_node_requests.clone();
            async move {
                loop {
                    let mut buf = [0; MAX_PACKET_SIZE];
                    let res = udp.recv_from(&mut buf).await;
                    match res {
                        Err(e) => {
                            warn!("UDP socket recv failure: {}", e);
                            break;
                        }
                        Ok((len, addr)) => {
                            let buf = &buf[..len];
                            if let Err(e) = async {
                                if addr.is_ipv6() {
                                    bail!("IPv6 is unsupported");
                                }

                                let min_len = 32 + 65 + 1;

                                if buf.len() < min_len {
                                    bail!("Packet too short: {} < {}", buf.len(), min_len);
                                }

                                let hash = keccak256(&buf[32..]);
                                let check_hash = H256::from_slice(&buf[0..32]);
                                if check_hash != hash {
                                    bail!(
                                        "Hash check failed: computed {}, prefix {}",
                                        hash,
                                        check_hash
                                    );
                                }

                                let rec_id = RecoveryId::from_i32(buf[96] as i32)?;
                                let rec_sig =
                                    RecoverableSignature::from_compact(&buf[32..96], rec_id)?;
                                let public_key =
                                    SECP256K1.recover_ecdsa(&keccak256_message(&buf[97..]), &rec_sig)?;
                                let remote_id = pk2id(&public_key);

                                if remote_id == id {
                                    return Ok(());
                                }

                                let typ = buf[97];
                                let mut data = &buf[98..];

                                async {
                                    match MessageId::from_u8(typ) {
                                        Some(MessageId::Ping) => {
                                            let ping_data_result = PingMessage::decode(&mut data);
                                            let ping_data = match ping_data_result {
                                                Err(DecodeError::Custom("from:empty")) => {
                                                    trace!("PING (ignore) due to an empty 'from' IP");
                                                    return Ok(())
                                                },
                                                other => other.context("RLP decoding of incoming Ping message data")?,
                                            };

                                            trace!("PING");

                                            connected.lock().add_verified(NodeRecord {
                                                address: ping_data.from.address,
                                                udp_port: ping_data.from.udp_port,
                                                tcp_port: ping_data.from.udp_port,
                                                id: remote_id,
                                            });

                                            let _ = egress_requests_tx
                                                .send((
                                                    addr,
                                                    remote_id,
                                                    EgressMessage::Pong(PongMessage {
                                                        to: ping_data.from,
                                                        echo: hash,
                                                        expire: ping_data.expire,
                                                    }),
                                                ))
                                                .await;

                                            if let Some(cbs) = expected_pings.lock().remove(&addr) {
                                                for (_, cb) in cbs {
                                                    let _ = cb.send(());
                                                }
                                            }
                                        }
                                        Some(MessageId::Pong) => {
                                            let message = PongMessage::decode(&mut data)
                                                .context("RLP decoding of incoming Pong message data")?;

                                            // Did we actually ask for this? Ignore message if not.
                                            if let Some(cbs) =
                                                inflight_ping_requests.lock().remove(&message.echo)
                                            {
                                                trace!("PONG - our endpoint is: {:?}", message.to);
                                                {
                                                    let mut node_endpoint = node_endpoint.write();
                                                    node_endpoint.address = message.to.address;
                                                    node_endpoint.udp_port = message.to.udp_port;
                                                }
                                                for cb in cbs {
                                                    let _ = cb.send(());
                                                }
                                            } else {
                                                trace!("PONG (unsolicited, ignoring)")
                                            }
                                        }
                                        Some(MessageId::FindNode) => {
                                            let message =
                                                FindNodeMessage::decode(&mut data)
                                                    .context("RLP decoding of incoming FindNode message data")?;

                                            let mut neighbours = None;
                                            {
                                                let connected = connected.lock();

                                                // Only send to nodes that have been proofed.
                                                if connected.get(remote_id).is_some() {
                                                    trace!("FINDNODE");
                                                    neighbours = connected
                                                        .neighbours(message.id)
                                                        .map(Box::new);
                                                } else {
                                                    trace!("FINDNODE (unproofed, ignoring)");
                                                }
                                            }

                                            if let Some(nodes) = neighbours {
                                                let _ = egress_requests_tx
                                                    .send((
                                                        addr,
                                                        remote_id,
                                                        EgressMessage::Neighbours(
                                                            NeighboursMessage {
                                                                nodes: nodes.into_iter().collect(),
                                                                expire: message.expire,
                                                            },
                                                        ),
                                                    ))
                                                    .await;
                                            }
                                        }
                                        Some(MessageId::Neighbours) => {
                                            // Did we actually ask for this? Ignore message if not.
                                            let cbs = inflight_find_node_requests.get(remote_id);
                                            if cbs.is_empty() {
                                                trace!("NEIGHBOURS (ignore)");
                                            } else {
                                                trace!("NEIGHBOURS");

                                                // OK, so we did ask, let's handle the message.
                                                let message =
                                                    NeighboursMessage::decode(&mut data)
                                                        .context("RLP decoding of incoming Neighbours message data")?;

                                                {
                                                    let mut connected = connected.lock();

                                                    for peer in message.nodes.iter() {
                                                        connected.add_seen(*peer);
                                                    }
                                                }

                                                for cb in cbs {
                                                    let _ = cb.send(message.clone()).await;
                                                }
                                            }
                                        }
                                        None => bail!("Invalid message type: {}", typ),
                                    };

                                    Ok(())
                                }
                                .instrument(span!(
                                    Level::TRACE,
                                    "HANDLER",
                                    "remote_id={}",
                                    &*remote_id.to_string()
                                ))
                                .await
                            }
                            .instrument(span!(Level::TRACE, "IN", "addr={}", &*addr.to_string()))
                            .await
                            {
                                trace!("Failed to handle message from {}: {}", addr, e);
                            }
                        }
                    }
                }
            }
        });

        let this = Arc::new(Self {
            task_group,
            connected,
            id,
            node_endpoint,
            egress_requests_tx,
            expected_pings,
            inflight_find_node_requests,
        });

        this.task_group.spawn_with_name("discv4 refresher", {
            let this = Arc::downgrade(&this);
            async move {
                while let Some(this) = this.upgrade() {
                    this.lookup_self().await;
                    drop(this);

                    sleep(REFRESH_TIMEOUT).await;
                }
            }
        });

        this.task_group
            .spawn_with_name("discv4 oldest node pinger", {
                let connected = this.connected.clone();
                let egress_requests_tx = this.egress_requests_tx.clone();
                let node_endpoint = this.node_endpoint.clone();
                async move {
                    loop {
                        let oldest = {
                            let connected = connected.lock();
                            connected
                                .filled_buckets()
                                .choose(&mut thread_rng())
                                .and_then(|bucket_no| connected.oldest(*bucket_no))
                        };

                        if let Some(node) = oldest {
                            let (tx, rx) = oneshot();
                            let from = *node_endpoint.read();
                            if egress_requests_tx
                                .send((
                                    node.udp_addr(),
                                    id,
                                    EgressMessage::Ping(
                                        PingMessage {
                                            from,
                                            to: node.into(),
                                            expire: ping_expiry(),
                                        },
                                        Some(tx),
                                    ),
                                ))
                                .await
                                .is_err()
                            {
                                return;
                            }

                            let _ = rx.await;
                        }

                        let sleep_duration = Duration::from_secs_f32(
                            PING_INTERVAL.as_secs_f32() * thread_rng().sample::<f32, _>(Standard),
                        );

                        sleep(sleep_duration).await;
                    }
                }
            });

        Ok(this)
    }

    pub async fn lookup_self(&self) -> Vec<NodeRecord> {
        self.lookup_inner(self.id).await
    }

    pub async fn lookup(&self, target: NodeId) -> Vec<NodeRecord> {
        self.lookup_inner(target).await
    }

    async fn lookup_inner(&self, target: NodeId) -> Vec<NodeRecord> {
        #[derive(Clone, Copy)]
        struct QueryNode {
            record: NodeRecord,
            queried: bool,
            responded: bool,
        }

        let node_endpoint = *self.node_endpoint.read();
        let egress_requests_tx = self.egress_requests_tx.clone();

        // Get all nodes from local table sorted by distance
        let mut nearest_nodes = self
            .connected
            .lock()
            .nearest_node_entries(target)
            .into_iter()
            .map(|(distance, record)| {
                (
                    distance,
                    QueryNode {
                        record,
                        queried: false,
                        responded: false,
                    },
                )
            })
            .take(ALPHA)
            .collect::<BTreeMap<_, _>>();
        let mut lookup_round = 0_usize;
        loop {
            // For each node of ALPHA closest and not queried yet...
            let picked_nodes = nearest_nodes
                .iter_mut()
                .take(ALPHA)
                .filter_map(|(distance, node)| {
                    if !node.queried {
                        Some((*distance, node))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();

            if picked_nodes.is_empty() {
                debug!("No picked nodes, ending lookup");
                break;
            }

            debug!("Picked alpha ({ALPHA}) closest nodes that are not queried");

            let fut = picked_nodes.into_iter().map(|(distance, node)| {
                // ...send find node request...
                node.queried = true;
                let node = *node;
                let egress_requests_tx = egress_requests_tx.clone();
                let expected_pings = self.expected_pings.clone();
                let inflight_find_node_requests = self.inflight_find_node_requests.clone();
                let expected_ping_id = rand::random();
                async move {
                    let addr = SocketAddr::new(node.record.address.0, node.record.udp_port);

                    let res = timeout(FIND_NODE_TIMEOUT, async {
                        let (expected_ping_tx, expected_ping_rx) = oneshot();
                        expected_pings
                            .lock()
                            .entry(addr)
                            .or_default()
                            .insert(expected_ping_id, expected_ping_tx);

                        // Make sure our endpoint is proven.
                        let (tx, rx) = oneshot();
                        egress_requests_tx
                            .send((
                                addr,
                                node.record.id,
                                EgressMessage::Ping(
                                    PingMessage {
                                        from: node_endpoint,
                                        to: node.record.into(),
                                        expire: ping_expiry(),
                                    },
                                    Some(tx),
                                ),
                            ))
                            .await
                            .map_err(|_| anyhow!("Sender shutdown"))?;
                        // ...and await for Pong response
                        rx.await.map_err(|_| anyhow!("Pong timeout"))?;

                        trace!("Our endpoint is proven");

                        // In case the node wants to ping us, give it an opportunity to do so
                        let _ = timeout(QUERY_AWAIT_PING_TIME, expected_ping_rx).await;

                        let (tx, mut rx) = channel(1);
                        let _guard = inflight_find_node_requests.add(node.record.id, tx);
                        egress_requests_tx
                            .send((
                                addr,
                                node.record.id,
                                EgressMessage::FindNode(FindNodeMessage {
                                    id: target,
                                    expire: find_node_expiry(),
                                }),
                            ))
                            .await
                            .map_err(|_| anyhow!("Sender shutdown"))?;

                        debug!("Awaiting neighbours");

                        // ...and await for Neighbours response
                        let mut received_neighbours = Vec::new();

                        while let Ok(neighbours) =
                            tokio::time::timeout(NEIGHBOURS_WAIT_TIMEOUT, rx.recv()).await
                        {
                            received_neighbours.push(
                                neighbours
                                    .expect("we drop the sending channel, not the ingress router"),
                            );
                        }
                        if received_neighbours.is_empty() {
                            bail!("No neigbours received");
                        }

                        debug!("Received neighbours");

                        Ok::<_, anyhow::Error>(received_neighbours)
                    })
                    .await;

                    if let hash_map::Entry::Occupied(mut entry) = expected_pings.lock().entry(addr)
                    {
                        entry.get_mut().remove(&expected_ping_id);
                        if entry.get().is_empty() {
                            entry.remove();
                        }
                    }

                    match res {
                        Ok(Ok(v)) => {
                            return Some((distance, v));
                        }
                        Ok(Err(e)) => {
                            debug!("Query error: {}", e);
                        }
                        Err(_) => {
                            debug!("Query timeout");
                        }
                    }

                    None
                }
                .instrument(span!(
                    Level::DEBUG,
                    "query",
                    "#{},node={}",
                    lookup_round,
                    node.record.id
                ))
            });

            for (d, messages) in join_all(fut).await.into_iter().flatten() {
                nearest_nodes
                    .get_mut(&d)
                    .expect("we just got this node from the nearest node set")
                    .responded = true;
                for message in messages {
                    // If we have a node...
                    for record in message.nodes.into_iter() {
                        // ...and it's not been seen yet...
                        if let btree_map::Entry::Vacant(vacant) =
                            nearest_nodes.entry(distance(target, record.id))
                        {
                            debug!("Adding unseen node to query: {:?}", record);
                            // ...add to the set and continue the query
                            vacant.insert(QueryNode {
                                record,
                                queried: false,
                                responded: false,
                            });
                        }
                    }
                }
            }

            lookup_round += 1;
        }

        nearest_nodes
            .into_iter()
            .filter_map(|(_, node)| {
                if node.responded {
                    Some(node.record)
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn num_nodes(&self) -> usize {
        self.connected.lock().len()
    }
}
