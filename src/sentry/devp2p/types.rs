use crate::sentry::devp2p::{peer::DisconnectReason, util::*};
use arrayvec::ArrayString;
use async_trait::async_trait;
use auto_impl::auto_impl;
use bytes::Bytes;
use derive_more::Display;
use educe::Educe;
pub use ethereum_types::H512 as PeerId;
use rlp::{DecoderError, Rlp, RlpStream};
use std::{collections::HashMap, fmt::Debug, future::pending, net::SocketAddr, str::FromStr};

/// Record that specifies information necessary to connect to RLPx node
#[derive(Clone, Copy, Debug)]
pub struct NodeRecord {
    /// Node ID.
    pub id: PeerId,
    /// Address of RLPx TCP server.
    pub addr: SocketAddr,
}

impl FromStr for NodeRecord {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        const PREFIX: &str = "enode://";

        let (prefix, data) = s.split_at(PREFIX.len());
        if prefix != PREFIX {
            return Err("Not an enode".into());
        }

        let mut parts = data.split('@');
        let id = parts.next().ok_or("Failed to read remote ID")?.parse()?;
        let addr = parts.next().ok_or("Failed to read address")?.parse()?;

        Ok(Self { id, addr })
    }
}

#[derive(Clone, Copy, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CapabilityName(pub ArrayString<4>);

impl rlp::Encodable for CapabilityName {
    fn rlp_append(&self, s: &mut RlpStream) {
        self.0.as_bytes().rlp_append(s);
    }
}

impl rlp::Decodable for CapabilityName {
    fn decode(rlp: &Rlp) -> Result<Self, DecoderError> {
        Ok(Self(
            ArrayString::from(
                std::str::from_utf8(rlp.data()?)
                    .map_err(|_| DecoderError::Custom("should be a UTF-8 string"))?,
            )
            .map_err(|_| DecoderError::RlpIsTooBig)?,
        ))
    }
}

pub type CapabilityLength = usize;
pub type CapabilityVersion = usize;

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
/// Capability information
pub struct CapabilityInfo {
    pub name: CapabilityName,
    pub version: CapabilityVersion,
    pub length: CapabilityLength,
}

impl CapabilityInfo {
    pub fn new(CapabilityId { name, version }: CapabilityId, length: CapabilityLength) -> Self {
        Self {
            name,
            version,
            length,
        }
    }
}

#[derive(Clone, Debug, Display, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[display(fmt = "{}/{}", name, version)]
pub struct CapabilityId {
    pub name: CapabilityName,
    pub version: CapabilityVersion,
}

impl From<CapabilityInfo> for CapabilityId {
    fn from(CapabilityInfo { name, version, .. }: CapabilityInfo) -> Self {
        Self { name, version }
    }
}

#[derive(Clone, Debug, Display)]
pub enum InboundEvent {
    #[display(
        fmt = "disconnect/{}",
        "reason.map(|r| r.to_string()).unwrap_or_else(|| \"(no reason)\".to_string())"
    )]
    Disconnect { reason: Option<DisconnectReason> },
    #[display(fmt = "message/{}/{}", capability_name, "message.id")]
    Message {
        capability_name: CapabilityName,
        message: Message,
    },
}

#[derive(Clone, Debug)]
pub enum OutboundEvent {
    Disconnect {
        reason: DisconnectReason,
    },
    Message {
        capability_name: CapabilityName,
        message: Message,
    },
}

#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait CapabilityServer: Send + Sync + 'static {
    /// Should be used to set up relevant state for the peer.
    fn on_peer_connect(&self, peer: PeerId, caps: HashMap<CapabilityName, CapabilityVersion>);
    /// Called on the next event for peer.
    async fn on_peer_event(&self, peer: PeerId, event: InboundEvent);
    /// Get the next event for peer.
    async fn next(&self, peer: PeerId) -> OutboundEvent;
}

#[async_trait]
impl CapabilityServer for () {
    fn on_peer_connect(&self, _: PeerId, _: HashMap<CapabilityName, CapabilityVersion>) {}

    async fn on_peer_event(&self, _: PeerId, _: InboundEvent) {}

    async fn next(&self, _: PeerId) -> OutboundEvent {
        pending().await
    }
}

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct Message {
    pub id: usize,
    #[educe(Debug(method = "hex_debug"))]
    pub data: Bytes,
}
