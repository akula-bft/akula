use crate::sentry::{
    devp2p::{PeerId, *},
    eth::*,
    CapabilityServerImpl,
};
use async_trait::async_trait;
use ethereum_interfaces::{
    sentry::{
        sentry_server::*, HandShakeReply, InboundMessage, MessageId as ProtoMessageId,
        OutboundMessageData, PeerMinBlockRequest, PeersReply, PeersRequest, SentPeers,
        SetStatusReply,
    },
    types::NodeInfoReply,
};
use futures_util::{stream::FuturesUnordered, Stream, TryStreamExt};
use num_traits::ToPrimitive;
use secp256k1::rand::seq::IteratorRandom;
use std::{collections::HashSet, convert::TryFrom, pin::Pin, sync::Arc};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    StreamExt,
};
use tonic::Response;
use tracing::*;

pub type InboundMessageStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<InboundMessage, tonic::Status>> + Send + Sync>>;

pub type PeersReplyStream =
    Pin<Box<dyn Stream<Item = anyhow::Result<PeersReply, tonic::Status>> + Send + Sync>>;

pub struct SentryService {
    capability_server: Arc<CapabilityServerImpl>,
}

impl SentryService {
    pub fn new(capability_server: Arc<CapabilityServerImpl>) -> Self {
        Self { capability_server }
    }
}

impl SentryService {
    async fn send_by_predicate<F, IT>(
        &self,
        request: Option<OutboundMessageData>,
        pred: F,
    ) -> SentPeers
    where
        F: FnOnce(&CapabilityServerImpl) -> IT,
        IT: IntoIterator<Item = PeerId>,
    {
        let result = self.try_send_by_predicate(request, pred).await;
        result.unwrap_or_else(|error| {
            warn!(
                "SentryService send_by_predicate ignores a message: {:?}",
                error
            );
            SentPeers { peers: Vec::new() }
        })
    }

    async fn try_send_by_predicate<F, IT>(
        &self,
        request: Option<OutboundMessageData>,
        pred: F,
    ) -> anyhow::Result<SentPeers>
    where
        F: FnOnce(&CapabilityServerImpl) -> IT,
        IT: IntoIterator<Item = PeerId>,
    {
        let request = request.ok_or_else(|| anyhow::anyhow!("empty request"))?;

        let proto_id = ProtoMessageId::from_i32(request.id)
            .ok_or_else(|| anyhow::anyhow!("unrecognized ProtoMessageId"))?;
        let eth_id = EthMessageId::try_from(proto_id)?;

        let message = Message {
            id: eth_id.to_usize().unwrap(),
            data: request.data,
        };

        let peers = (pred)(&*self.capability_server)
            .into_iter()
            .map(|peer| {
                let message = message.clone();
                async move { self.send_message(message, peer).await }
            })
            .collect::<FuturesUnordered<_>>()
            .filter_map(|res| res.ok()) // ignore errors
            .map(|peer_id| peer_id.into())
            .collect::<Vec<_>>()
            .await;

        Ok(SentPeers { peers })
    }

    async fn send_message(&self, message: Message, peer: PeerId) -> anyhow::Result<PeerId> {
        let sender = self
            .capability_server
            .sender(peer)
            .ok_or_else(|| anyhow::anyhow!("sender not found for peer"))?;

        let outbound_message = OutboundEvent::Message {
            capability_name: capability_name(),
            message,
        };

        let result = sender.send(outbound_message).await;

        match result {
            Ok(_) => Ok(peer),
            Err(error) => Err(anyhow::anyhow!(error)),
        }
    }
}

#[async_trait]
impl Sentry for SentryService {
    async fn penalize_peer(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::PenalizePeerRequest>,
    ) -> Result<Response<()>, tonic::Status> {
        let peer = request
            .into_inner()
            .peer_id
            .ok_or_else(|| tonic::Status::invalid_argument("no peer id"))?
            .into();
        if let Some(sender) = self.capability_server.sender(peer) {
            let _ = sender
                .send(OutboundEvent::Disconnect {
                    reason: DisconnectReason::DisconnectRequested,
                })
                .await;
        }

        Ok(Response::new(()))
    }

    async fn peer_count(
        &self,
        _: tonic::Request<ethereum_interfaces::sentry::PeerCountRequest>,
    ) -> Result<Response<ethereum_interfaces::sentry::PeerCountReply>, tonic::Status> {
        let reply = ethereum_interfaces::sentry::PeerCountReply {
            count: self.capability_server.all_peers().len() as u64,
        };
        Ok(Response::new(reply))
    }

    type PeersStream = PeersReplyStream;

    async fn peers(
        &self,
        _request: tonic::Request<PeersRequest>,
    ) -> Result<Response<Self::PeersStream>, tonic::Status> {
        let receiver = self.capability_server.peers_status_sender.subscribe();
        let stream = BroadcastStream::new(receiver)
            // map BroadcastStreamRecvError to tonic::Status
            .map_err(|error| match error {
                BroadcastStreamRecvError::Lagged(_) => tonic::Status::new(
                    tonic::Code::ResourceExhausted,
                    "The receiver lagged too far behind. Some events dropped.",
                ),
            });
        Ok(Response::new(Box::pin(stream)))
    }

    async fn send_message_by_min_block(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::SendMessageByMinBlockRequest>,
    ) -> Result<Response<SentPeers>, tonic::Status> {
        let ethereum_interfaces::sentry::SendMessageByMinBlockRequest { data, min_block } =
            request.into_inner();
        Ok(Response::new(
            self.send_by_predicate(data, |capability_server| {
                capability_server
                    .block_tracker
                    .read()
                    .peers_with_min_block(min_block)
            })
            .await,
        ))
    }

    async fn send_message_by_id(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::SendMessageByIdRequest>,
    ) -> Result<Response<SentPeers>, tonic::Status> {
        let ethereum_interfaces::sentry::SendMessageByIdRequest { peer_id, data } =
            request.into_inner();

        let peer = peer_id
            .ok_or_else(|| tonic::Status::invalid_argument("no peer id"))?
            .into();

        Ok(Response::new(
            self.send_by_predicate(data, |_| std::iter::once(peer))
                .await,
        ))
    }

    async fn send_message_to_random_peers(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::SendMessageToRandomPeersRequest>,
    ) -> Result<Response<SentPeers>, tonic::Status> {
        let ethereum_interfaces::sentry::SendMessageToRandomPeersRequest { max_peers, data } =
            request.into_inner();

        Ok(Response::new(
            self.send_by_predicate(data, |capability_server| {
                let peers = capability_server.all_peers();
                let amount = usize::min(max_peers as usize, peers.len());
                peers
                    .into_iter()
                    .choose_multiple(&mut secp256k1::rand::thread_rng(), amount)
            })
            .await,
        ))
    }

    async fn send_message_to_all(
        &self,
        request: tonic::Request<OutboundMessageData>,
    ) -> Result<Response<SentPeers>, tonic::Status> {
        Ok(Response::new(
            self.send_by_predicate(Some(request.into_inner()), |capability_server| {
                capability_server.all_peers()
            })
            .await,
        ))
    }

    async fn peer_min_block(
        &self,
        request: tonic::Request<PeerMinBlockRequest>,
    ) -> Result<Response<()>, tonic::Status> {
        let PeerMinBlockRequest { peer_id, min_block } = request.into_inner();

        let peer = peer_id
            .ok_or_else(|| tonic::Status::invalid_argument("no peer id"))?
            .into();

        self.capability_server
            .block_tracker
            .write()
            .set_block_number(peer, min_block, false);

        Ok(Response::new(()))
    }

    async fn set_status(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::StatusData>,
    ) -> Result<Response<SetStatusReply>, tonic::Status> {
        let s = FullStatusData::try_from(request.into_inner())
            .map_err(|e| tonic::Status::invalid_argument(e.to_string()))?;

        self.capability_server.set_status(s);

        Ok(Response::new(SetStatusReply {}))
    }

    async fn hand_shake(
        &self,
        _request: tonic::Request<()>,
    ) -> Result<Response<HandShakeReply>, tonic::Status> {
        let protocol_version = self.capability_server.protocol_version;
        let reply = HandShakeReply {
            protocol: ethereum_interfaces::sentry::Protocol::from(protocol_version) as i32,
        };

        Ok(Response::new(reply))
    }

    type MessagesStream = InboundMessageStream;

    async fn messages(
        &self,
        request: tonic::Request<ethereum_interfaces::sentry::MessagesRequest>,
    ) -> Result<Response<Self::MessagesStream>, tonic::Status> {
        let ids_set = request
            .into_inner()
            .ids
            .into_iter()
            .collect::<HashSet<i32>>();

        let receiver = self.capability_server.data_sender.subscribe();
        let stream = BroadcastStream::new(receiver)
            .filter_map(|res| res.ok()) // ignore BroadcastStreamRecvError
            .filter(move |message: &InboundMessage| {
                ids_set.is_empty() || ids_set.contains(&message.id)
            })
            .map(Ok);

        Ok(Response::new(Box::pin(stream)))
    }

    async fn node_info(
        &self,
        _: tonic::Request<()>,
    ) -> Result<Response<NodeInfoReply>, tonic::Status> {
        Err(tonic::Status::unimplemented("todo"))
    }
}
