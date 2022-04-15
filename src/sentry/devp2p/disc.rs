use crate::sentry::devp2p::types::*;
use async_stream::stream;
use futures::{stream::BoxStream, StreamExt};
use std::{collections::HashMap, net::SocketAddr, pin::Pin, task::Poll, time::Duration};
use tokio::time::sleep;
use tokio_stream::Stream;

pub mod v4;
pub use self::v4::{Discv4, Discv4Builder};

pub mod dns;

pub use self::dns::DnsDiscovery;

pub type Discovery = BoxStream<'static, anyhow::Result<NodeRecord>>;

pub struct StaticNodes(Pin<Box<dyn Stream<Item = anyhow::Result<NodeRecord>> + Send + 'static>>);

impl StaticNodes {
    pub fn new(nodes: HashMap<SocketAddr, PeerId>, delay: Duration) -> Self {
        Self(Box::pin(stream! {
            loop {
                for (&addr, &id) in &nodes {
                    yield Ok(NodeRecord { id, addr });
                    sleep(delay).await;
                }
            }
        }))
    }
}

impl Stream for StaticNodes {
    type Item = anyhow::Result<NodeRecord>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}
