use super::{NodeId, NodeRecord};
use derive_more::*;
use fastrlp::*;
use primitive_types::H256;
use std::net::IpAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Deref, DerefMut, From)]
pub struct Ip(pub IpAddr);

impl Encodable for Ip {
    fn encode(&self, out: &mut dyn BufMut) {
        match self.0 {
            IpAddr::V4(addr) => addr.octets().encode(out),
            IpAddr::V6(addr) => addr.octets().encode(out),
        }
    }

    fn length(&self) -> usize {
        match self.0 {
            IpAddr::V4(addr) => addr.octets().length(),
            IpAddr::V6(addr) => addr.octets().length(),
        }
    }
}

impl Decodable for Ip {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        match Header::decode(&mut &**buf)?.payload_length {
            0 => Err(DecodeError::Custom("empty")),
            4 => Ok(Self(IpAddr::from(<[u8; 4]>::decode(buf)?))),
            16 => Ok(Self(IpAddr::from(<[u8; 16]>::decode(buf)?))),
            other => {
                tracing::debug!("ip_addr_rlp_decode: wrong address length {other}");
                Err(DecodeError::Custom("wrong IP address length"))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, RlpEncodable, RlpDecodable)]
pub struct Endpoint {
    pub address: Ip,
    pub udp_port: u16,
    pub tcp_port: u16,
}

impl From<NodeRecord> for Endpoint {
    fn from(
        NodeRecord {
            address,
            tcp_port,
            udp_port,
            ..
        }: NodeRecord,
    ) -> Self {
        Self {
            address,
            udp_port,
            tcp_port,
        }
    }
}

#[derive(Clone, Copy, Debug, RlpEncodable, RlpDecodable)]
pub struct FindNodeMessage {
    pub id: NodeId,
    pub expire: u64,
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable)]
pub struct NeighboursMessage {
    pub nodes: Vec<NodeRecord>,
    pub expire: u64,
}

#[derive(Debug, Clone)]
pub struct PingMessage {
    pub from: Endpoint,
    pub to: Endpoint,
    pub expire: u64,
}

#[derive(RlpEncodable)]
struct PingMessageE<'s> {
    version: u64,
    from: &'s Endpoint,
    to: &'s Endpoint,
    expire: &'s u64,
}

impl Encodable for PingMessage {
    fn encode(&self, out: &mut dyn BufMut) {
        let Self { from, to, expire } = self;

        PingMessageE {
            version: 4,
            from,
            to,
            expire,
        }
        .encode(out)
    }
    fn length(&self) -> usize {
        let Self { from, to, expire } = self;

        PingMessageE {
            version: 4,
            from,
            to,
            expire,
        }
        .length()
    }
}

#[derive(RlpDecodable)]
struct PingMessageD {
    version: u64,
    from: Endpoint,
    to: Endpoint,
    expire: u64,
}

impl Decodable for PingMessage {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let PingMessageD {
            from, to, expire, ..
        } = PingMessageD::decode(buf)?;

        Ok(Self { from, to, expire })
    }
}

#[derive(Debug, Clone, RlpEncodable, RlpDecodable)]
pub struct PongMessage {
    pub to: Endpoint,
    pub echo: H256,
    pub expire: u64,
}
