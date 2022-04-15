use crate::{
    models::{Block, BlockNumber, ChainConfig, H256, U256},
    p2p::types::*,
};
use arrayvec::ArrayVec;
use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::BytesMut;
use ethereum_interfaces::sentry as grpc_sentry;
use fastrlp::Encodable;
use futures::{stream::FuturesUnordered, Stream};
use std::{pin::Pin, sync::atomic::AtomicU64};
use tokio_stream::StreamExt;
use tracing::error;

pub const BATCH_SIZE: usize = 3 << 15;
pub const CHUNK_SIZE: usize = 1 << 10;

pub type SentryClient = grpc_sentry::sentry_client::SentryClient<tonic::transport::Channel>;

#[derive(Debug, Clone)]
pub struct SentryPool(Vec<SentryClient>);

impl From<SentryClient> for SentryPool {
    #[inline]
    fn from(sentry: SentryClient) -> Self {
        Self(vec![sentry])
    }
}
impl const From<Vec<SentryClient>> for SentryPool {
    #[inline]
    fn from(sentries: Vec<SentryClient>) -> Self {
        Self(sentries)
    }
}

#[derive(Debug)]
pub struct Peer {
    pub conn: Vec<SentryClient>,
    pub chain_config: ChainConfig,
    pub status: AtomicStatus,
    pub ping_counter: AtomicU64,
    pub forks: Vec<u64>,
}

impl Peer {
    #[inline]
    pub fn new<T>(conn: T, chain_config: ChainConfig, status: Status) -> Self
    where
        T: Into<SentryPool>,
    {
        let forks = chain_config
            .forks()
            .iter()
            .map(|&number| number.0)
            .collect::<Vec<_>>();

        Self {
            conn: conn.into().0,
            chain_config,
            status: AtomicStatus::new(status),
            ping_counter: AtomicU64::new(0),
            forks,
        }
    }

    /// Returns latest ping value.
    #[inline]
    pub fn last_ping(&self) -> BlockNumber {
        BlockNumber(self.ping_counter.load(std::sync::atomic::Ordering::SeqCst))
    }
}

pub type HashChunk = ArrayVec<H256, CHUNK_SIZE>;

pub type SentryInboundStream = futures::stream::Map<
    tonic::Streaming<grpc_sentry::InboundMessage>,
    fn(anyhow::Result<grpc_sentry::InboundMessage, tonic::Status>) -> Option<InboundMessage>,
>;

pub struct SentryStream(tonic::codec::Streaming<grpc_sentry::InboundMessage>);
pub type InboundStream = futures::stream::SelectAll<SentryStream>;

#[inline]
async fn recv_sentry(conn: &SentryClient, ids: Vec<i32>) -> anyhow::Result<SentryStream> {
    let mut conn = conn.clone();
    conn.hand_shake(tonic::Request::new(())).await?;

    Ok(SentryStream(
        conn.messages(grpc_sentry::MessagesRequest { ids })
            .await?
            .into_inner(),
    ))
}

impl Stream for SentryStream {
    type Item = InboundMessage;

    #[inline]
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = unsafe { Pin::new_unchecked(&mut self.get_mut().0) };

        match this.poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(value))) => match InboundMessage::try_from(value) {
                Ok(msg) => std::task::Poll::Ready(Some(msg)),
                _ => std::task::Poll::Pending,
            },
            _ => std::task::Poll::Pending,
        }
    }
}

#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait PeerExt: Send + Sync {
    async fn set_status(&self) -> anyhow::Result<()>;
    async fn send_message(&self, message: Message, predicate: PeerFilter) -> anyhow::Result<()>;
    async fn send_ping(&self) -> anyhow::Result<()>;
    async fn send_body_request(&self, req: Vec<H256>) -> anyhow::Result<()>;
    async fn send_header_request(&self, req: HeaderRequest) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<InboundStream>;
    async fn recv_headers(&self) -> anyhow::Result<InboundStream>;
    async fn recv_bodies(&self) -> anyhow::Result<InboundStream>;
    async fn broadcast_block(&self, block: Block, td: u128) -> anyhow::Result<()>;
    async fn broadcast_new_block_hashes(
        &self,
        block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()>;
    async fn propagate_transactions(&self, transactions: Vec<H256>) -> anyhow::Result<()>;
    async fn update_head(
        &self,
        height: BlockNumber,
        hash: H256,
        total_difficulty: U256,
    ) -> anyhow::Result<()>;
    async fn penalize_peer(&self, penalty: Penalty) -> anyhow::Result<()>;
    async fn peer_count(&self) -> anyhow::Result<u64>;
}

#[async_trait]
impl PeerExt for Peer {
    #[inline]
    async fn set_status(&self) -> anyhow::Result<()> {
        let Status {
            height,
            hash,
            total_difficulty,
        } = self.status.load();
        let status_data = grpc_sentry::StatusData {
            network_id: self.chain_config.network_id().0,
            total_difficulty: Some(total_difficulty.into()),
            best_hash: Some(hash.into()),
            fork_data: Some(grpc_sentry::Forks {
                genesis: Some(self.chain_config.genesis_hash.into()),
                forks: self.forks.clone(),
            }),
            max_block: *height,
        };

        let _ = self
            .conn
            .iter()
            .map(|conn| {
                // Little thing to avoid lifetime issues, since we always want to do things in parallel.
                let mut conn = conn.clone();
                let status_data = status_data.clone();

                tokio::spawn(async move {
                    // We'll add logging by abusing some nightly features.
                    if let Err(err) = conn.hand_shake(tonic::Request::new(())).await {
                        // All errors are omitted on purpose, because we always do auto reconnect
                        // so there's no reason to go down.
                        error!("Sentry handshake failed: {:?}", err);
                    };
                    // And the same for the status itself.
                    if let Err(err) = conn.set_status(tonic::Request::new(status_data)).await {
                        error!("Sentry status update failed: {:?}", err);
                    };
                })
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>();

        Ok(())
    }

    #[inline]
    async fn send_message(&self, msg: Message, predicate: PeerFilter) -> anyhow::Result<()> {
        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(msg.id()) as i32,
            data: |msg: Message| -> bytes::Bytes {
                let mut buf = BytesMut::new();
                msg.encode(&mut buf);
                buf.freeze()
            }(msg),
        };

        let send_msg = move |mut conn: SentryClient,
                             predicate: PeerFilter,
                             data: grpc_sentry::OutboundMessageData| {
            async move {
                conn.hand_shake(tonic::Request::new(())).await?;
                match predicate {
                    PeerFilter::All => {
                        conn.send_message_to_all(data).await?;
                    }
                    PeerFilter::Random(max_peers) => {
                        conn.send_message_to_random_peers(
                            grpc_sentry::SendMessageToRandomPeersRequest {
                                data: Some(data),
                                max_peers,
                            },
                        )
                        .await?;
                    }
                    PeerFilter::PeerId(peer_id) => {
                        conn.send_message_by_id(grpc_sentry::SendMessageByIdRequest {
                            data: Some(data),
                            peer_id: Some(peer_id.into()),
                        })
                        .await?;
                    }
                    PeerFilter::MinBlock(min_block) => {
                        conn.send_message_by_min_block(grpc_sentry::SendMessageByMinBlockRequest {
                            data: Some(data),
                            min_block,
                        })
                        .await?;
                    }
                };
                Ok::<_, anyhow::Error>(())
            }
        };

        let _ = self
            .conn
            .iter()
            .map(|conn| {
                let conn = conn.clone();
                let predicate = predicate.clone();
                let data = data.clone();
                tokio::spawn(async move {
                    let _ = send_msg(conn, predicate, data).await;
                })
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>();

        Ok(())
    }

    #[inline]
    async fn send_ping(&self) -> anyhow::Result<()> {
        let _ = self
            .send_header_request(HeaderRequest {
                start: BlockId::Number(BlockNumber(
                    self.ping_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                )),
                limit: 1,
                ..Default::default()
            })
            .await;

        Ok(())
    }
    #[inline]
    async fn send_body_request(&self, request: Vec<H256>) -> anyhow::Result<()> {
        self.set_status().await?;
        self.send_message(request.into(), PeerFilter::All).await?;

        Ok(())
    }
    #[inline]
    async fn send_header_request(&self, request: HeaderRequest) -> anyhow::Result<()> {
        self.set_status().await?;
        self.send_message(request.into(), PeerFilter::All).await?;

        Ok(())
    }
    #[inline]
    async fn recv(&self) -> anyhow::Result<InboundStream> {
        self.set_status().await?;

        Ok(futures::stream::select_all(
            futures::future::join_all(
                self.conn
                    .iter()
                    .map(|conn| {
                        recv_sentry(
                            conn,
                            vec![
                                grpc_sentry::MessageId::from(MessageId::NewBlockHashes) as i32,
                                grpc_sentry::MessageId::from(MessageId::NewBlock) as i32,
                                grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32,
                                grpc_sentry::MessageId::from(MessageId::BlockBodies) as i32,
                            ],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .filter_map(Result::ok),
        ))
    }
    #[inline]
    async fn recv_headers(&self) -> anyhow::Result<InboundStream> {
        self.set_status().await?;

        Ok(futures::stream::select_all(
            futures::future::join_all(
                self.conn
                    .iter()
                    .map(|conn| {
                        recv_sentry(
                            conn,
                            vec![grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .filter_map(Result::ok),
        ))
    }
    #[inline]
    async fn recv_bodies(&self) -> anyhow::Result<InboundStream> {
        self.set_status().await?;

        Ok(futures::stream::select_all(
            futures::future::join_all(
                self.conn
                    .iter()
                    .map(|conn| {
                        recv_sentry(
                            conn,
                            vec![grpc_sentry::MessageId::from(MessageId::BlockBodies) as i32],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .into_iter()
            .filter_map(Result::ok),
        ))
    }
    #[inline]
    async fn broadcast_block(&self, _: Block, _: u128) -> anyhow::Result<()> {
        todo!();
    }
    #[inline]
    async fn broadcast_new_block_hashes(&self, _: Vec<(H256, BlockNumber)>) -> anyhow::Result<()> {
        todo!();
    }
    #[inline]
    async fn propagate_transactions(&self, _: Vec<H256>) -> anyhow::Result<()> {
        todo!()
    }
    #[inline]
    async fn update_head(
        &self,
        height: BlockNumber,
        hash: H256,
        total_difficulty: U256,
    ) -> anyhow::Result<()> {
        let status = Status {
            height,
            hash,
            total_difficulty: H256::from_slice(&total_difficulty.to_be_bytes()),
        };
        self.status.store(status);
        self.set_status().await?;
        Ok(())
    }
    #[inline]
    async fn penalize_peer(&self, penalty: Penalty) -> anyhow::Result<()> {
        let _ = self
            .conn
            .iter()
            .map(|sentry| {
                let mut sentry = sentry.clone();
                tokio::spawn(async move {
                    if let Err(err) = sentry
                        .penalize_peer(grpc_sentry::PenalizePeerRequest {
                            peer_id: Some(penalty.peer_id.into()),
                            penalty: 0,
                        })
                        .await
                    {
                        error!("Could not penalize peer: {:?}", err)
                    }
                })
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<Vec<_>>();

        Ok(())
    }
    #[inline]
    async fn peer_count(&self) -> anyhow::Result<u64> {
        todo!()
    }
}
