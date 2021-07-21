use crate::downloader::{
    message_decoder, messages::*, sentry_address::SentryAddress, sentry_client::*,
};
use async_trait::async_trait;
use ethereum_interfaces::{sentry as grpc_sentry, types as grpc_types};
use futures_core::Stream;
use std::convert::TryFrom;
use tokio_stream::StreamExt;
use tracing::*;

pub struct SentryClientImpl {
    client: grpc_sentry::sentry_client::SentryClient<tonic::transport::channel::Channel>,
}

impl SentryClientImpl {
    pub async fn new(addr: SentryAddress) -> anyhow::Result<Self> {
        info!("SentryClient connecting to {}...", addr.addr);
        let client = grpc_sentry::sentry_client::SentryClient::connect(addr.addr).await?;
        Ok(SentryClientImpl { client })
    }
}

#[async_trait]
impl SentryClient for SentryClientImpl {
    async fn set_status(&mut self, status: Status) -> anyhow::Result<()> {
        let fork_data = grpc_sentry::Forks {
            genesis: Some(status.chain_fork_config.genesis_block_hash.into()),
            forks: status.chain_fork_config.fork_block_numbers,
        };

        let status_data = grpc_sentry::StatusData {
            network_id: u64::from(status.chain_fork_config.id.0),
            total_difficulty: Some(grpc_types::H256::from(status.total_difficulty)),
            best_hash: Some(grpc_types::H256::from(status.best_hash)),
            fork_data: Some(fork_data),
            max_block: status.max_block,
        };
        let request = tonic::Request::new(status_data);
        let response = self.client.set_status(request).await?;
        let reply: grpc_sentry::SetStatusReply = response.into_inner();
        debug!("SentryClient set_status replied with: {:?}", reply);
        return Ok(());
    }

    async fn send_message(
        &mut self,
        message: Message,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<u32> {
        let message_id = message.eth_id();
        let message_data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(message_id) as i32,
            data: rlp::encode(&message).into(),
        };

        let response = match peer_filter {
            PeerFilter::MinBlock(min_block) => {
                let request = grpc_sentry::SendMessageByMinBlockRequest {
                    data: Some(message_data),
                    min_block,
                };
                self.client
                    .send_message_by_min_block(tonic::Request::new(request))
                    .await?
            }
            PeerFilter::PeerId(peer_id) => {
                let request = grpc_sentry::SendMessageByIdRequest {
                    data: Some(message_data),
                    peer_id: Some(grpc_types::H512::from(peer_id)),
                };
                self.client
                    .send_message_by_id(tonic::Request::new(request))
                    .await?
            }
            PeerFilter::Random(max_peers) => {
                let request = grpc_sentry::SendMessageToRandomPeersRequest {
                    data: Some(message_data),
                    max_peers,
                };
                self.client
                    .send_message_to_random_peers(tonic::Request::new(request))
                    .await?
            }
            PeerFilter::All => {
                let request = message_data;
                self.client
                    .send_message_to_all(tonic::Request::new(request))
                    .await?
            }
        };
        let sent_peers: grpc_sentry::SentPeers = response.into_inner();
        debug!(
            "SentryClient send_message sent {:?} to: {:?}",
            message.eth_id(),
            sent_peers
        );
        let sent_peers_count = sent_peers.peers.len() as u32;
        return Ok(sent_peers_count);
    }

    async fn receive_messages(
        &mut self,
        filter_ids: &[EthMessageId],
    ) -> anyhow::Result<Box<dyn Stream<Item = anyhow::Result<MessageFromPeer>> + Unpin + Send>>
    {
        let grpc_ids = filter_ids
            .iter()
            .map(|id| grpc_sentry::MessageId::from(*id) as i32)
            .collect::<Vec<_>>();
        let ids_request = grpc_sentry::MessagesRequest { ids: grpc_ids };
        let request = tonic::Request::new(ids_request);
        let response = self.client.messages(request).await?;
        let tonic_stream: tonic::codec::Streaming<grpc_sentry::InboundMessage> =
            response.into_inner();
        let tonic_stream = tonic_stream_fuse_on_error(tonic_stream);
        debug!("SentryClient receive_messages subscribed to incoming messages");

        let stream = tonic_stream.map(|result: Result<grpc_sentry::InboundMessage, tonic::Status>| -> anyhow::Result<MessageFromPeer> {
            match result {
                Ok(inbound_message) => {
                    let grpc_message_id = grpc_sentry::MessageId::from_i32(inbound_message.id)
                        .ok_or_else(|| anyhow::anyhow!("SentryClient receive_messages stream got an invalid MessageId {}", inbound_message.id))?;
                    let message_id = EthMessageId::try_from(grpc_message_id)?;
                    let grpc_peer_id: Option<grpc_types::H512> = inbound_message.peer_id;
                    let peer_id: Option<ethereum_types::H512> = grpc_peer_id.map(ethereum_types::H512::from);
                    let message_bytes: static_bytes::Bytes = inbound_message.data;
                    let message = message_decoder::decode_rlp_message(message_id, message_bytes.as_ref())?;
                    let message_from_peer = MessageFromPeer {
                        message,
                        from_peer_id: peer_id,
                    };
                    debug!("SentryClient receive_messages received a message {:?} from {:?}",
                        message_from_peer.message.eth_id(),
                        message_from_peer.from_peer_id);
                    Ok(message_from_peer)
                },
                Err(status) => {
                    if status.message().ends_with("broken pipe") {
                        Err(anyhow::Error::new(std::io::Error::new(std::io::ErrorKind::BrokenPipe, status)))
                    } else {
                        Err(anyhow::Error::new(status))
                    }
                }
            }
        });
        Ok(Box::new(stream))
    }
}

fn tonic_stream_fuse_on_error<T: 'static + Send>(
    mut tonic_stream: tonic::codec::Streaming<T>,
) -> Box<dyn Stream<Item = Result<T, tonic::Status>> + Unpin + Send> {
    let stream = async_stream::stream! {
        while let Some(result) = tonic_stream.next().await {
            match result {
                Ok(item) => {
                    yield Ok(item);
                },
                Err(status) => {
                    yield Err(status);
                    break;
                },
            }
        }
    };
    Box::new(Box::pin(stream))
}

impl From<EthMessageId> for grpc_sentry::MessageId {
    fn from(id: EthMessageId) -> Self {
        match id {
            EthMessageId::Status => grpc_sentry::MessageId::Status66,
            EthMessageId::NewBlockHashes => grpc_sentry::MessageId::NewBlockHashes66,
            EthMessageId::Transactions => grpc_sentry::MessageId::Transactions66,
            EthMessageId::GetBlockHeaders => grpc_sentry::MessageId::GetBlockHeaders66,
            EthMessageId::BlockHeaders => grpc_sentry::MessageId::BlockHeaders66,
            EthMessageId::GetBlockBodies => grpc_sentry::MessageId::GetBlockBodies66,
            EthMessageId::BlockBodies => grpc_sentry::MessageId::BlockBodies66,
            EthMessageId::NewBlock => grpc_sentry::MessageId::NewBlock66,
            EthMessageId::NewPooledTransactionHashes => {
                grpc_sentry::MessageId::NewPooledTransactionHashes66
            }
            EthMessageId::GetPooledTransactions => grpc_sentry::MessageId::GetPooledTransactions66,
            EthMessageId::PooledTransactions => grpc_sentry::MessageId::PooledTransactions66,
            EthMessageId::GetNodeData => grpc_sentry::MessageId::GetNodeData66,
            EthMessageId::NodeData => grpc_sentry::MessageId::NodeData66,
            EthMessageId::GetReceipts => grpc_sentry::MessageId::GetReceipts66,
            EthMessageId::Receipts => grpc_sentry::MessageId::Receipts66,
        }
    }
}

impl TryFrom<grpc_sentry::MessageId> for EthMessageId {
    type Error = anyhow::Error;

    fn try_from(id: grpc_sentry::MessageId) -> anyhow::Result<Self> {
        match id {
            grpc_sentry::MessageId::Status66 => Ok(EthMessageId::Status),
            grpc_sentry::MessageId::NewBlockHashes66 => Ok(EthMessageId::NewBlockHashes),
            grpc_sentry::MessageId::Transactions66 => Ok(EthMessageId::Transactions),
            grpc_sentry::MessageId::GetBlockHeaders66 => Ok(EthMessageId::GetBlockHeaders),
            grpc_sentry::MessageId::BlockHeaders66 => Ok(EthMessageId::BlockHeaders),
            grpc_sentry::MessageId::GetBlockBodies66 => Ok(EthMessageId::GetBlockBodies),
            grpc_sentry::MessageId::BlockBodies66 => Ok(EthMessageId::BlockBodies),
            grpc_sentry::MessageId::NewBlock66 => Ok(EthMessageId::NewBlock),
            grpc_sentry::MessageId::NewPooledTransactionHashes66 => {
                Ok(EthMessageId::NewPooledTransactionHashes)
            }
            grpc_sentry::MessageId::GetPooledTransactions66 => {
                Ok(EthMessageId::GetPooledTransactions)
            }
            grpc_sentry::MessageId::PooledTransactions66 => Ok(EthMessageId::PooledTransactions),
            grpc_sentry::MessageId::GetNodeData66 => Ok(EthMessageId::GetNodeData),
            grpc_sentry::MessageId::NodeData66 => Ok(EthMessageId::NodeData),
            grpc_sentry::MessageId::GetReceipts66 => Ok(EthMessageId::GetReceipts),
            grpc_sentry::MessageId::Receipts66 => Ok(EthMessageId::Receipts),
            _ => Err(anyhow::anyhow!("unsupported MessageId '{:?}'", id)),
        }
    }
}
