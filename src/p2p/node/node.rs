#![allow(unreachable_code)]

use super::{stash::Stash, stream::*};
use crate::{
    models::{BlockNumber, ChainConfig, MessageWithSignature, H256},
    p2p::types::*,
};
use bytes::{BufMut, BytesMut};
use dashmap::DashSet;
use ethereum_interfaces::{
    sentry as grpc_sentry,
    sentry::{sentry_client::SentryClient, PeerMinBlockRequest, SentPeers},
};
use ethereum_types::H512;
use fastrlp::*;
use futures::stream::FuturesUnordered;
use hashlink::LruCache;
use parking_lot::{Mutex, RwLock};
use rand::{thread_rng, Rng};
use std::{
    collections::HashSet,
    future::{pending, Future},
    sync::Arc,
    time::Duration,
};
use task_group::TaskGroup;
use tokio::sync::{watch, Notify};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::*;

pub type Sentry = SentryClient<Channel>;
pub type SentryId = usize;

pub type PeerId = H512;

pub type RequestId = u64;

#[derive(Debug)]
pub struct Node {
    pub stash: Arc<dyn Stash>,
    /// The sentry clients.
    pub sentries: Vec<Sentry>,
    /// The current Node status message.
    pub status: RwLock<Status>,
    /// Node chain config.
    pub config: ChainConfig,
    /// Highest persistent chain tip.
    pub chain_tip: watch::Receiver<(BlockNumber, H256)>,
    pub chain_tip_sender: watch::Sender<(BlockNumber, H256)>,
    /// Block cache
    pub block_cache: Mutex<LruCache<H256, (SentryId, PeerId, crate::models::Block)>>,
    pub block_cache_notify: Notify,
    /// Table of block hashes of the blocks known to not belong to the canonical chain.
    pub bad_blocks: DashSet<H256>,
    /// Chain forks.
    pub forks: Vec<u64>,
}

impl Node {
    const SYNC_INTERVAL: Duration = Duration::from_secs(5);

    /// Start node synchronization.
    pub async fn start_sync(self: Arc<Self>, tip_discovery: bool) -> anyhow::Result<()> {
        let tasks = TaskGroup::new();

        let (tx, mut rx) = tokio::sync::mpsc::channel(128);
        let requested = Arc::new(Mutex::new(LruCache::new(128)));

        tasks.spawn({
            let handler = self.clone();
            let requested = requested.clone();

            async move {
                let mut stream = handler.sync_stream().await;
                loop {
                    if let Some(msg) = stream.next().await {
                        let peer_id = msg.peer_id;
                        let sentry_id = msg.sentry_id;

                        match msg.msg {
                            Message::NewBlockHashes(ref blocks) => {
                                let mut max_block = None;
                                for b in &blocks.0 {
                                    if tip_discovery && b.number > handler.chain_tip.borrow().0 {
                                        let id = thread_rng().gen::<u64>();
                                        tx.send((
                                            id,
                                            PeerFilter::Peer(peer_id, sentry_id),
                                            b.hash,
                                            0u64,
                                        ))
                                        .await?;
                                        requested.lock().insert(id, ());
                                    }
                                    max_block = std::cmp::max(max_block, Some(b.number));
                                }

                                if let Some(max_block) = max_block {
                                    let _ = handler.sentries[sentry_id]
                                        .clone()
                                        .peer_min_block(PeerMinBlockRequest {
                                            peer_id: Some(peer_id.into()),
                                            min_block: *max_block,
                                        })
                                        .await;
                                }
                            }
                            Message::BlockHeaders(ref headers) => {
                                if let Some(max_header) =
                                    headers.headers.iter().max_by_key(|header| header.number)
                                {
                                    let _ = handler.sentries[sentry_id]
                                        .clone()
                                        .peer_min_block(PeerMinBlockRequest {
                                            peer_id: Some(peer_id.into()),
                                            min_block: max_header.number.0,
                                        })
                                        .await;
                                }

                                if tip_discovery
                                    && requested.lock().remove(&headers.request_id).is_some()
                                    && headers.headers.len() == 1
                                {
                                    let header = &headers.headers[0];
                                    let hash = header.hash();

                                    if header.number > handler.chain_tip.borrow().0 {
                                        let _ =
                                            handler.chain_tip_sender.send((header.number, hash));
                                        for skip in 1..4_u64 {
                                            let id = rand::thread_rng().gen::<u64>();
                                            tx.send((id, PeerFilter::All, hash, skip)).await?;
                                            requested.lock().insert(id, ());
                                        }
                                    }
                                }
                            }
                            Message::NewBlock(inner) => {
                                let hash = inner.block.header.hash();
                                let number = inner.block.header.number;

                                handler
                                    .block_cache
                                    .lock()
                                    .insert(hash, (sentry_id, peer_id, inner.block));
                                handler.block_cache_notify.notify_one();

                                if tip_discovery && number > handler.chain_tip.borrow().0 {
                                    let _ = handler.chain_tip_sender.send((number, hash));
                                    for skip in 1..4_u64 {
                                        let id = rand::thread_rng().gen::<u64>();
                                        tx.send((id, PeerFilter::All, hash, skip)).await?;
                                        requested.lock().insert(id, ());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            }
        });

        let _ = tasks.spawn({
            let handler = self.clone();

            async move {
                while let Some((request_id, filter, hash, skip)) = rx.recv().await {
                    let msg = Message::GetBlockHeaders(GetBlockHeaders {
                        request_id,
                        params: HeaderRequest {
                            start: BlockId::Hash(hash),
                            limit: 1,
                            reverse: false,
                            skip,
                        }
                        .into(),
                    });
                    let _ = handler.send_message(msg, filter).await;
                }

                Ok::<(), anyhow::Error>(())
            }
        });

        if tip_discovery {
            let _ = tasks.spawn({
                let handler = self.clone();
                async move {
                    loop {
                        let (block_number, _) = *handler.chain_tip.borrow();

                        for skip in 1..4 {
                            let request_id = rand::thread_rng().gen::<u64>();
                            requested.lock().insert(request_id, ());

                            let msg = Message::GetBlockHeaders(GetBlockHeaders {
                                request_id,
                                params: HeaderRequest {
                                    start: BlockId::Number(block_number),
                                    limit: 1,
                                    reverse: false,
                                    skip,
                                }
                                .into(),
                            });

                            let _ = handler.send_message(msg, PeerFilter::All).await;
                        }

                        tokio::time::sleep(Self::SYNC_INTERVAL).await;
                    }
                    Ok::<(), anyhow::Error>(())
                }
            });
        }

        tasks.spawn({
            let handler = self.clone();
            let mut stream = handler
                .stream_by_predicate([
                    ethereum_interfaces::sentry::MessageId::GetBlockBodies66 as i32,
                    ethereum_interfaces::sentry::MessageId::GetBlockHeaders66 as i32,
                ])
                .await;

            async move {
                while let Some(msg) = stream.next().await {
                    let peer_id = msg.peer_id;
                    let sentry_id = msg.sentry_id;

                    match msg.msg {
                        Message::GetBlockHeaders(inner) => {
                            let msg = Message::BlockHeaders(BlockHeaders {
                                request_id: inner.request_id,
                                headers: handler
                                    .stash
                                    .get_headers(inner.params)
                                    .unwrap_or_default(),
                            });

                            handler
                                .send_message(msg, PeerFilter::Peer(peer_id, sentry_id))
                                .await;
                        }
                        Message::GetBlockBodies(inner) => {
                            let msg = Message::BlockBodies(BlockBodies {
                                request_id: inner.request_id,
                                bodies: handler.stash.get_bodies(inner.hashes).unwrap_or_default(),
                            });

                            handler
                                .send_message(msg, PeerFilter::Peer(peer_id, sentry_id))
                                .await;
                        }
                        _ => unreachable!(),
                    }
                }

                Ok::<(), anyhow::Error>(())
            }
        });

        pending::<()>().await;

        Ok(())
    }

    /// Marks block with given hash as non-canonical.
    pub fn mark_bad_block(&self, hash: H256) {
        self.bad_blocks.insert(hash);
    }

    /// Updates current node status.
    pub async fn update_chain_head(&self, status: Option<Status>) {
        if let Some(val) = status {
            *self.status.write() = val;
        }

        let Status {
            height,
            hash,
            total_difficulty,
        } = *self.status.read();
        let config = &self.config;
        let status_data = grpc_sentry::StatusData {
            network_id: *config.network_id(),
            total_difficulty: Some(total_difficulty.into()),
            best_hash: Some(hash.into()),
            fork_data: Some(grpc_sentry::Forks {
                genesis: Some(config.genesis_hash.into()),
                forks: self.forks.clone(),
            }),
            max_block: *height,
        };
        self.set_status(status_data).await
    }

    pub async fn send_message(
        &self,
        msg: Message,
        pred: PeerFilter,
    ) -> HashSet<(SentryId, PeerId)> {
        debug!("Sending message: {msg:?} to peers {pred:?}");
        let id = grpc_sentry::MessageId::from(msg.id()) as i32;
        let data = || -> bytes::Bytes {
            let mut buf = BytesMut::new();
            msg.encode(&mut buf);
            buf.freeze()
        }();

        self.send_raw(grpc_sentry::OutboundMessageData { id, data }, pred)
            .await
    }

    pub async fn send_many_header_requests<T>(
        self: Arc<Self>,
        requests: T,
    ) -> HashSet<(SentryId, PeerId)>
    where
        T: IntoIterator<Item = HeaderRequest>,
    {
        requests
            .into_iter()
            .map(|request| {
                let node = self.clone();
                tokio::spawn(async move {
                    trace!("Sending header request: {request:?}");
                    node.send_header_request(None, request, None).await
                })
            })
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(|res| res.ok())
            .flatten()
            .collect()
    }

    pub async fn send_header_request(
        &self,
        request_id: Option<u64>,
        request: HeaderRequest,
        max_block: Option<BlockNumber>,
    ) -> HashSet<(SentryId, PeerId)> {
        self.send_message(
            Message::GetBlockHeaders(GetBlockHeaders {
                request_id: request_id.unwrap_or_else(|| rand::thread_rng().gen::<u64>()),
                params: request.into(),
            }),
            if let Some(max_block) = max_block {
                PeerFilter::MinBlock(max_block.0)
            } else {
                PeerFilter::All
            },
        )
        .await
    }

    /// Sends a block bodies request to other peers.
    pub async fn send_block_request<'a>(
        &self,
        request_id: u64,
        blocks: &'a [(BlockNumber, H256)],
        min_block_filter: bool,
    ) -> Option<(SentryId, PeerId)> {
        self.update_chain_head(None).await;

        let hashes = blocks.iter().map(|(_, h)| *h).collect::<Vec<_>>();
        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(MessageId::GetBlockBodies) as i32,
            data: {
                let mut buf = BytesMut::new();
                GetBlockBodies { request_id, hashes }.encode(&mut buf);
                buf.freeze()
            },
        };
        let filter = if min_block_filter {
            let min_block = blocks
                .iter()
                .min_by_key(|(block_number, _)| block_number)
                .unwrap()
                .0;
            PeerFilter::MinBlock(*min_block)
        } else {
            PeerFilter::Random(1)
        };
        self.send_raw(data, filter).await.into_iter().next()
    }

    pub async fn send_pooled_transactions(
        &self,
        request_id: RequestId,
        transactions: Vec<MessageWithSignature>,
        pred: PeerFilter,
    ) -> HashSet<(SentryId, PeerId)> {
        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(MessageId::PooledTransactions) as i32,
            data: |transactions: Vec<MessageWithSignature>| -> bytes::Bytes {
                let mut buf = BytesMut::new();
                PooledTransactions {
                    request_id,
                    transactions,
                }
                .encode(&mut buf);
                buf.freeze()
            }(transactions),
        };
        self.send_raw(data, pred).await
    }

    pub async fn get_pooled_transactions<'a>(
        &self,
        request_id: u64,
        hashes: &'a [H256],
        pred: PeerFilter,
    ) -> HashSet<(SentryId, PeerId)> {
        pub struct GetPooledTransactions_<'a> {
            pub request_id: u64,
            pub hashes: &'a [H256],
        }
        trait E {
            fn rlp_header(&self) -> fastrlp::Header;
        }
        impl<'a> E for GetPooledTransactions_<'a> {
            fn rlp_header(&self) -> fastrlp::Header {
                let mut rlp_head = fastrlp::Header {
                    list: true,
                    payload_length: 0,
                };
                rlp_head.payload_length += fastrlp::Encodable::length(&self.request_id);
                rlp_head.payload_length += fastrlp::list_length(self.hashes);
                rlp_head
            }
        }
        impl<'a> Encodable for GetPooledTransactions_<'a> {
            fn length(&self) -> usize {
                let rlp_head = E::rlp_header(self);
                fastrlp::length_of_length(rlp_head.payload_length) + rlp_head.payload_length
            }
            fn encode(&self, out: &mut dyn BufMut) {
                E::rlp_header(self).encode(out);
                fastrlp::Encodable::encode(&self.request_id, out);
                fastrlp::encode_list(self.hashes, out);
            }
        }

        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(MessageId::GetPooledTransactions) as i32,
            data: || -> bytes::Bytes {
                let mut buf = BytesMut::new();
                GetPooledTransactions_ { request_id, hashes }.encode(&mut buf);
                buf.freeze()
            }(),
        };
        self.send_raw(data, pred).await
    }

    const SYNC_PREDICATE: [i32; 3] = [
        grpc_sentry::MessageId::BlockHeaders66 as i32,
        grpc_sentry::MessageId::NewBlockHashes66 as i32,
        grpc_sentry::MessageId::NewBlock66 as i32,
    ];
    async fn sync_stream(&self) -> NodeStream {
        self.update_chain_head(None).await;

        let sentries = self.sentries.iter().collect::<Vec<_>>();
        SentryStream::join_all(sentries, Self::SYNC_PREDICATE).await
    }

    const RAW_PREDICATE: [i32; 4] = [
        grpc_sentry::MessageId::NewBlockHashes66 as i32,
        grpc_sentry::MessageId::NewBlock66 as i32,
        grpc_sentry::MessageId::BlockHeaders66 as i32,
        grpc_sentry::MessageId::BlockBodies66 as i32,
    ];

    pub async fn stream_raw(&self) -> NodeStream {
        self.update_chain_head(None).await;

        let sentries = self.sentries.iter().collect::<Vec<_>>();
        SentryStream::join_all(sentries, Self::RAW_PREDICATE).await
    }

    const TRANSACTIONS_PREDICATE: [i32; 4] = [
        grpc_sentry::MessageId::Transactions66 as i32,
        grpc_sentry::MessageId::NewPooledTransactionHashes66 as i32,
        grpc_sentry::MessageId::PooledTransactions66 as i32,
        grpc_sentry::MessageId::GetPooledTransactions66 as i32,
    ];

    pub async fn stream_transactions(&self) -> NodeStream {
        SentryStream::join_all(self.sentries.iter(), Self::TRANSACTIONS_PREDICATE).await
    }

    pub async fn stream_by_predicate<T>(&self, pred: T) -> NodeStream
    where
        T: IntoIterator<Item = i32>,
    {
        SentryStream::join_all(self.sentries.iter(), pred).await
    }

    const HEADERS_PREDICATE: [i32; 1] = [grpc_sentry::MessageId::BlockHeaders66 as i32];

    pub async fn stream_headers(&self) -> NodeStream {
        let sentries = self.sentries.iter().collect::<Vec<_>>();
        SentryStream::join_all(sentries, Self::HEADERS_PREDICATE).await
    }

    const BODIES_PREDICATE: [i32; 1] = [grpc_sentry::MessageId::BlockBodies66 as i32];

    pub async fn stream_bodies(&self) -> NodeStream {
        self.update_chain_head(None).await;

        let sentries = self.sentries.iter().collect::<Vec<_>>();
        SentryStream::join_all(sentries, Self::BODIES_PREDICATE).await
    }

    #[inline]
    pub async fn sqrt_peers(&self) -> usize {
        (self.total_peers().await as f64).sqrt() as usize
    }

    pub async fn total_peers(&self) -> usize {
        let mut s = self
            .sentries
            .clone()
            .into_iter()
            .map(|mut sentry| async move {
                tokio::time::timeout(
                    Duration::from_secs(2),
                    sentry.peer_count(grpc_sentry::PeerCountRequest {}),
                )
                .await
            })
            .collect::<FuturesUnordered<_>>();

        let mut sum = 0;
        while let Some(Ok(Ok(s))) = s.next().await {
            sum += s.into_inner().count.try_into().unwrap_or(0);
        }

        sum
    }

    pub async fn penalize_peer(&self, peer_id: impl Into<ethereum_interfaces::types::H512>) {
        let request = grpc_sentry::PenalizePeerRequest {
            peer_id: Some(peer_id.into()),
            penalty: 0i32,
        };

        self.sentries
            .clone()
            .into_iter()
            .map(|mut sentry| {
                let request = request.clone();
                async move {
                    let _ = sentry.penalize_peer(request).await;
                }
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>()
            .await
    }
}

impl Node {
    const TIMEOUT: Duration = Duration::from_secs(2);

    async fn send_raw(
        &self,
        data: impl Into<grpc_sentry::OutboundMessageData>,
        predicate: PeerFilter,
    ) -> HashSet<(SentryId, PeerId)> {
        let data = data.into();

        async fn map_await<T, I, F>(
            iter: T,
            data: grpc_sentry::OutboundMessageData,
            closure: F,
        ) -> HashSet<(SentryId, PeerId)>
        where
            T: IntoIterator<Item = (SentryId, Sentry)>,
            I: Future<Output = Result<tonic::Response<SentPeers>, tonic::Status>>,
            F: Fn(Sentry, grpc_sentry::OutboundMessageData) -> I,
        {
            iter.into_iter()
                .map(|(id, sentry)| {
                    let data = data.clone();
                    let fut = closure(sentry, data);
                    async move {
                        if let Ok(Ok(v)) = tokio::time::timeout(Node::TIMEOUT, fut).await {
                            return v
                                .into_inner()
                                .peers
                                .into_iter()
                                .map(|peer_id| (id, peer_id.into()))
                                .collect();
                        }

                        HashSet::new()
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .flatten()
                .collect()
        }

        match predicate {
            PeerFilter::All => {
                map_await(
                    self.sentries.iter().cloned().enumerate(),
                    data,
                    |mut sentry, data| async move { sentry.send_message_to_all(data).await },
                )
                .await
            }
            PeerFilter::Random(max_peers) => {
                map_await(
                    self.sentries.iter().cloned().enumerate(),
                    data,
                    |mut sentry, data| async move {
                        sentry
                            .send_message_to_random_peers(
                                grpc_sentry::SendMessageToRandomPeersRequest {
                                    data: Some(data),
                                    max_peers,
                                },
                            )
                            .await
                    },
                )
                .await
            }
            PeerFilter::Peer(peer_id, sentry_id) => {
                let iter = std::iter::once((sentry_id, self.sentries[sentry_id].clone()));
                map_await(iter, data, |mut sentry, data| async move {
                    sentry
                        .send_message_by_id(grpc_sentry::SendMessageByIdRequest {
                            data: Some(data),
                            peer_id: Some(peer_id.into()),
                        })
                        .await
                })
                .await
            }
            PeerFilter::MinBlock(min_block) => {
                map_await(
                    self.sentries.iter().cloned().enumerate(),
                    data,
                    |mut sentry, data| async move {
                        sentry
                            .send_message_by_min_block(grpc_sentry::SendMessageByMinBlockRequest {
                                data: Some(data),
                                min_block,
                            })
                            .await
                    },
                )
                .await
            }
        }
    }
    async fn set_status(&self, status_data: grpc_sentry::StatusData) {
        self.sentries
            .clone()
            .into_iter()
            .map(move |mut sentry| {
                let status_data = status_data.clone();

                async move {
                    if let Err(err) = sentry.hand_shake(tonic::Request::new(())).await {
                        error!("Failed to handshake with sentry: {:?}", err);
                    };
                    if let Err(err) = sentry.set_status(tonic::Request::new(status_data)).await {
                        error!("Failed to set sentry status: {:?}", err);
                    }
                }
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>()
            .await;
    }
}
