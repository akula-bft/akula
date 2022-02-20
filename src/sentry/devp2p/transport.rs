use async_trait::async_trait;
use cidr::IpCidr;
use std::{fmt::Debug, net::SocketAddr};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};
use tracing::*;

pub trait Transport: AsyncRead + AsyncWrite + Debug + Send + Unpin + 'static {
    fn remote_addr(&self) -> Option<SocketAddr>;
}

impl Transport for TcpStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.peer_addr().ok()
    }
}

#[async_trait]
pub trait TcpServer {
    type Conn: Transport;

    async fn accept(&self) -> anyhow::Result<Self::Conn>;
}

#[async_trait]
impl TcpServer for tokio::net::TcpListener {
    type Conn = tokio::net::TcpStream;

    async fn accept(&self) -> anyhow::Result<Self::Conn> {
        Ok(tokio::net::TcpListener::accept(self).await?.0)
    }
}

pub struct TokioCidrListener {
    tcp_server: tokio::net::TcpListener,
    cidr_mask: Option<IpCidr>,
}

impl TokioCidrListener {
    pub fn new(tcp_server: tokio::net::TcpListener, cidr_mask: Option<IpCidr>) -> Self {
        Self {
            tcp_server,
            cidr_mask,
        }
    }
}

#[async_trait]
impl TcpServer for TokioCidrListener {
    type Conn = tokio::net::TcpStream;

    async fn accept(&self) -> anyhow::Result<Self::Conn> {
        loop {
            let (node, remote_addr) = self.tcp_server.accept().await?;

            if let Some(cidr) = &self.cidr_mask {
                if !cidr.contains(&remote_addr.ip()) {
                    debug!(
                        "Ignoring connection request: {} is not in range {}",
                        remote_addr, cidr
                    );
                } else {
                    return Ok(node);
                }
            }
        }
    }
}
