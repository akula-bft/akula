use crate::downloader::{sentry_address::SentryAddress, sentry_client::*};
use async_trait::async_trait;
use ethereum_interfaces::{sentry as grpc_sentry, types as grpc_types};
use rlp;
use static_bytes;
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
        message: Box<dyn Message>,
        peer_filter: PeerFilter,
    ) -> anyhow::Result<()> {
        let message_bytes: static_bytes::BytesMut = rlp::encode(&message);
        let message_data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(message.id()) as i32,
            data: static_bytes::Bytes::from(message_bytes),
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
            message.id(),
            sent_peers
        );
        return Ok(());
    }
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
