#![allow(dead_code, clippy::upper_case_acronyms)]

use self::eth::*;
use async_stream::stream;
use async_trait::async_trait;
use devp2p::*;
use educe::Educe;
use ethereum_interfaces::sentry::{self, InboundMessage, PeersReply};
use fastrlp::Decodable;
use futures_core::stream::BoxStream;
use num_traits::{FromPrimitive, ToPrimitive};
use parking_lot::RwLock;
use std::{
    collections::{btree_map::Entry, hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet},
    fmt::Debug,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{
    broadcast::{channel as broadcast_channel, Sender as BroadcastSender},
    mpsc::{channel, Sender},
    Mutex as AsyncMutex,
};
use tokio_stream::StreamExt;
use tracing::*;

pub mod devp2p;
pub mod eth;
pub mod grpc;
pub mod services;

type OutboundSender = Sender<OutboundEvent>;
type OutboundReceiver = Arc<AsyncMutex<BoxStream<'static, OutboundEvent>>>;

pub const BUFFERING_FACTOR: usize = 5;

#[derive(Clone)]
struct Pipes {
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
    peer_pipes: Arc<RwLock<HashMap<PeerId, Pipes>>>,
    block_tracker: Arc<RwLock<BlockTracker>>,

    status_message: Arc<RwLock<Option<FullStatusData>>>,
    protocol_version: EthProtocolVersion,
    valid_peers: Arc<RwLock<HashSet<PeerId>>>,

    data_sender: BroadcastSender<InboundMessage>,
    peers_status_sender: BroadcastSender<PeersReply>,

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
                .send(ethereum_interfaces::sentry::PeersReply {
                    peer_id: Some(ethereum_interfaces::types::H512::from(peer)),
                    event: ethereum_interfaces::sentry::peers_reply::PeerEvent::Disconnect as i32,
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

                        let status_data = self.status_message.read();
                        let mut valid_peers = self.valid_peers.write();
                        if let Some(FullStatusData { fork_filter, .. }) = &*status_data {
                            fork_filter.validate(v.fork_id).map_err(|reason| {
                                debug!("Kicking peer with incompatible fork ID: {:?}", reason);

                                DisconnectReason::UselessPeer
                            })?;

                            valid_peers.insert(peer);

                            let send_status_result =
                                self.peers_status_sender
                                    .send(ethereum_interfaces::sentry::PeersReply {
                                    peer_id: Some(ethereum_interfaces::types::H512::from(peer)),
                                    event:
                                        ethereum_interfaces::sentry::peers_reply::PeerEvent::Connect
                                            as i32,
                                });
                            if send_status_result.is_err() {
                                debug!("No subscribers to report peer status to");
                            }
                        }
                    }
                    Some(inbound_id) if valid_peer => {
                        if self
                            .data_sender
                            .send(InboundMessage {
                                id: sentry::MessageId::from(inbound_id) as i32,
                                data,
                                peer_id: Some(peer.into()),
                            })
                            .is_err()
                        {
                            warn!("no connected sentry, dropping status");
                            *self.status_message.write() = None;
                        }
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
