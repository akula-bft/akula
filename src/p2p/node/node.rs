#![allow(unreachable_code)]

use super::{stash::Stash, stream::*};
use crate::{
    models::{BlockNumber, ChainConfig, MessageWithSignature, H256},
    p2p::{node::NodeBuilder, types::*},
};
use bytes::{BufMut, BytesMut};
use dashmap::DashSet;
use ethereum_interfaces::{sentry as grpc_sentry, sentry::sentry_client::SentryClient};
use fastrlp::*;
use futures::stream::FuturesUnordered;
use hashlink::LruCache;
use parking_lot::{Mutex, RwLock};
use rand::{thread_rng, Rng};
use std::{future::pending, sync::Arc, time::Duration};
use task_group::TaskGroup;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::*;

pub type Sentry = SentryClient<Channel>;
pub type RequestId = u64;

#[derive(Debug)]
pub(crate) struct BlockCaches {
    /// Mapping from the child hash to it's parent.
    pub(crate) parent_cache: LruCache<H256, H256>,
    /// Mapping from the block hash to it's number.
    pub(crate) block_cache: LruCache<H256, BlockNumber>,
}

#[derive(Debug)]
pub struct Node {
    pub(crate) stash: Arc<dyn Stash>,
    /// The sentry clients.
    pub(crate) sentries: Vec<Sentry>,
    /// The current Node status message.
    pub(crate) status: RwLock<Status>,
    /// Node chain config.
    pub(crate) config: ChainConfig,
    /// Highest persistent chain tip.
    pub(crate) chain_tip: RwLock<(BlockNumber, H256)>,
    /// Block caches
    pub(crate) block_caches: Mutex<BlockCaches>,
    /// Table of block hashes of the blocks known to not belong to the canonical chain.
    pub(crate) bad_blocks: DashSet<H256>,
    /// Chain forks.
    pub(crate) forks: Vec<u64>,
}

impl Node {
    /// Node builder.
    pub fn builder() -> NodeBuilder {
        NodeBuilder::default()
    }
}

impl Node {
    const SYNC_INTERVAL: Duration = Duration::from_secs(5);

    /// Start node synchronization.
    pub async fn start_sync(self: Arc<Self>) -> anyhow::Result<()> {
        let tasks = TaskGroup::new();

        let (tx, mut rx) = tokio::sync::mpsc::channel(128);

        let requested = Arc::new(Mutex::new(LruCache::new(128)));

        let _ = tasks.spawn({
            let handler = self.clone();
            let requested = requested.clone();

            async move {
                let mut stream = handler.sync_stream().await;
                loop {
                    if let Some(msg) = stream.next().await {
                        let (block_number, _) = *handler.chain_tip.read();
                        let peer_id = msg.peer_id;

                        match msg.msg {
                            Message::NewBlockHashes(ref blocks) => {
                                for b in &blocks.0 {
                                    if b.number > block_number {
                                        let id = thread_rng().gen::<u64>();
                                        tx.send((id, PeerFilter::PeerId(peer_id), b.hash, 0u64))
                                            .await?;
                                        requested.lock().insert(id, ());
                                    }
                                }
                            }
                            Message::BlockHeaders(ref headers)
                                if requested.lock().remove(&headers.request_id).is_some()
                                    && headers.headers.len() == 1 =>
                            {
                                let header = &headers.headers[0];
                                let hash = header.hash();

                                {
                                    let mut caches = handler.block_caches.lock();

                                    caches.block_cache.insert(hash, header.number);
                                    caches.block_cache.insert(
                                        header.parent_hash,
                                        BlockNumber(header.number.checked_sub(1).unwrap_or(0)),
                                    );

                                    caches.parent_cache.insert(hash, header.parent_hash);
                                }

                                if header.number > block_number {
                                    *handler.chain_tip.write() = (header.number, hash);
                                    for skip in 1..4_u64 {
                                        let id = rand::thread_rng().gen::<u64>();
                                        tx.send((id, PeerFilter::All, hash, skip)).await?;
                                        requested.lock().insert(id, ());
                                    }
                                }
                            }
                            Message::NewBlock(inner) => {
                                let hash = inner.block.header.hash();
                                let number = inner.block.header.number;
                                let parent_hash = inner.block.header.parent_hash;

                                {
                                    let mut caches = handler.block_caches.lock();
                                    caches.block_cache.insert(hash, number);
                                    caches.block_cache.insert(
                                        parent_hash,
                                        BlockNumber(number.checked_sub(1).unwrap_or(0)),
                                    );

                                    caches.parent_cache.insert(hash, parent_hash);
                                }

                                if number > block_number {
                                    *handler.chain_tip.write() = (inner.block.header.number, hash);
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
                Ok::<(), anyhow::Error>(())
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

        let _ = tasks.spawn({
            let handler = self.clone();
            async move {
                loop {
                    let (block_number, _) = *handler.chain_tip.read();

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

                    match msg.msg {
                        Message::GetBlockHeaders(inner) => {
                            let msg = Message::BlockHeaders(BlockHeaders {
                                request_id: inner.request_id,
                                headers: self.stash.get_headers(inner.params).unwrap_or_default(),
                            });

                            self.send_message(msg, PeerFilter::PeerId(peer_id)).await?;
                        }
                        Message::GetBlockBodies(inner) => {
                            let msg = Message::BlockBodies(BlockBodies {
                                request_id: inner.request_id,
                                bodies: self.stash.get_bodies(inner.hashes).unwrap_or_default(),
                            });

                            self.send_message(msg, PeerFilter::PeerId(peer_id)).await?;
                        }
                        _ => unreachable!(),
                    }
                }

                Ok::<(), anyhow::Error>(())
            }
            .instrument(span!(Level::DEBUG, "inbound handler"))
        });

        pending::<()>().await;

        Ok(())
    }

    /// Marks block with given hash as non-canonical.
    pub fn mark_bad_block(&self, hash: H256) {
        self.bad_blocks.insert(hash);
    }

    /// Finds first bad block if any, and returns it's index in given iterable.
    #[inline]
    pub fn position_bad_block<'a, T: Iterator<Item = &'a H256>>(&self, iter: T) -> Option<usize> {
        iter.into_iter().position(|h| self.bad_blocks.contains(h))
    }

    /// Updates current node status.
    pub async fn update_chain_head(&self, status: Option<Status>) -> anyhow::Result<()> {
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
        self.set_status(status_data).await?;
        Ok(())
    }

    pub async fn send_message(&self, msg: Message, pred: PeerFilter) -> anyhow::Result<()> {
        let id = grpc_sentry::MessageId::from(msg.id()) as i32;
        let data = || -> bytes::Bytes {
            let mut buf = BytesMut::new();
            msg.encode(&mut buf);
            buf.freeze()
        }();

        self.send_raw(grpc_sentry::OutboundMessageData { id, data }, pred)
            .await?;

        Ok(())
    }

    pub async fn send_many_header_requests<T>(self: Arc<Self>, requests: T) -> anyhow::Result<()>
    where
        T: IntoIterator<Item = HeaderRequest>,
    {
        requests
            .into_iter()
            .map(|request| {
                let node = self.clone();
                tokio::spawn(async move { node.send_header_request(request).await })
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>()
            .await;
        Ok(())
    }

    pub async fn send_header_request(&self, request: HeaderRequest) -> anyhow::Result<()> {
        self.send_message(request.into(), PeerFilter::All).await?;

        Ok(())
    }

    /// Sends a block bodies request to other peers.
    pub async fn send_block_request<'a>(&self, hashes: &'a [H256]) -> anyhow::Result<()> {
        self.update_chain_head(None).await?;

        let request_id = rand::thread_rng().gen::<u64>();
        pub struct GetBlockBodies_<'a> {
            pub request_id: u64,
            pub hashes: &'a [H256],
        }
        trait E {
            fn rlp_header(&self) -> fastrlp::Header;
        }
        impl<'a> E for GetBlockBodies_<'a> {
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
        impl<'a> Encodable for GetBlockBodies_<'a> {
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
            id: grpc_sentry::MessageId::from(MessageId::GetBlockBodies) as i32,
            data: |hashes: &'_ [H256]| -> bytes::Bytes {
                let mut buf = BytesMut::new();
                GetBlockBodies_ { request_id, hashes }.encode(&mut buf);
                buf.freeze()
            }(hashes),
        };
        self.send_raw(data, PeerFilter::All).await?;

        Ok(())
    }

    pub async fn send_pooled_transactions(
        &self,
        request_id: RequestId,
        transactions: Vec<MessageWithSignature>,
        pred: PeerFilter,
    ) -> anyhow::Result<()> {
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
        self.send_raw(data, pred).await?;

        Ok(())
    }

    pub async fn get_pooled_transactions<'a>(
        &self,
        request_id: u64,
        hashes: &'a [H256],
        pred: PeerFilter,
    ) -> anyhow::Result<()> {
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
            data: |hashes: &'_ [H256]| -> bytes::Bytes {
                let mut buf = BytesMut::new();
                GetPooledTransactions_ { request_id, hashes }.encode(&mut buf);
                buf.freeze()
            }(hashes),
        };
        self.send_raw(data, pred).await?;

        Ok(())
    }

    const SYNC_PREDICATE: [i32; 3] = [
        grpc_sentry::MessageId::BlockHeaders66 as i32,
        grpc_sentry::MessageId::NewBlockHashes66 as i32,
        grpc_sentry::MessageId::NewBlock66 as i32,
    ];
    async fn sync_stream(&self) -> NodeStream {
        let _ = self.update_chain_head(None).await;

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
        let _ = self.update_chain_head(None).await;

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
        let _ = self.update_chain_head(None).await;

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

    pub async fn penalize_peer(
        &self,
        peer_id: impl Into<ethereum_interfaces::types::H512>,
    ) -> anyhow::Result<()> {
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
            .await;

        Ok(())
    }
}

impl Node {
    async fn send_raw(
        &self,
        data: impl Into<grpc_sentry::OutboundMessageData>,
        predicate: PeerFilter,
    ) -> anyhow::Result<()> {
        let send_msg = move |mut sentry: Sentry,
                             pred: PeerFilter,
                             data: grpc_sentry::OutboundMessageData| async move {
            match pred {
                PeerFilter::All => {
                    sentry.send_message_to_all(data).await?;
                }
                PeerFilter::Random(max_peers) => {
                    sentry
                        .send_message_to_random_peers(
                            grpc_sentry::SendMessageToRandomPeersRequest {
                                data: Some(data),
                                max_peers,
                            },
                        )
                        .await?;
                }
                PeerFilter::PeerId(peer_id) => {
                    sentry
                        .send_message_by_id(grpc_sentry::SendMessageByIdRequest {
                            data: Some(data),
                            peer_id: Some(peer_id.into()),
                        })
                        .await?;
                }
                PeerFilter::MinBlock(min_block) => {
                    sentry
                        .send_message_by_min_block(grpc_sentry::SendMessageByMinBlockRequest {
                            data: Some(data),
                            min_block,
                        })
                        .await?;
                }
            };
            Ok::<_, anyhow::Error>(())
        };

        const TIMEOUT: Duration = Duration::from_secs(2);

        let data = data.into();
        self.sentries
            .clone()
            .into_iter()
            .map(|sentry| {
                let predicate = predicate.clone();
                let data = data.clone();

                async move {
                    let _ =
                        tokio::time::timeout(TIMEOUT, send_msg(sentry, predicate, data)).await?;
                    Ok::<_, anyhow::Error>(())
                }
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>()
            .await;

        Ok(())
    }
    async fn set_status(&self, status_data: grpc_sentry::StatusData) -> anyhow::Result<()> {
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
        Ok(())
    }
}
