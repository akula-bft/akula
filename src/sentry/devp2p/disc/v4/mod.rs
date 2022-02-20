//! Ethereum Node Discovery v4 implementation.

#![allow(clippy::type_complexity)]

mod kad;
mod message;
mod node;
mod proto;
mod util;

use educe::Educe;
use ethereum_types::H512;
use std::{pin::Pin, sync::Arc};
use task_group::TaskGroup;
use tokio::sync::mpsc::{channel, Receiver};
use tokio_stream::Stream;

pub type NodeId = H512;
pub use self::node::{Node, NodeRecord};

#[derive(Educe)]
#[educe(Default)]
pub struct Discv4Builder {
    #[educe(Default(1))]
    concurrent_lookups: usize,
    #[educe(Default(20))]
    cache: usize,
}

impl Discv4Builder {
    pub fn with_concurrent_lookups(mut self, concurrent_lookups: usize) -> Self {
        self.concurrent_lookups = concurrent_lookups;
        self
    }

    pub fn with_cache(mut self, cache: usize) -> Self {
        self.cache = cache;
        self
    }

    pub fn build(self, node: Arc<Node>) -> Discv4 {
        Discv4::new(node, self.concurrent_lookups, self.cache)
    }
}

pub struct Discv4 {
    #[allow(unused)]
    tasks: TaskGroup,
    receiver: Receiver<crate::sentry::devp2p::types::NodeRecord>,
}

impl Discv4 {
    #[must_use]
    fn new(node: Arc<Node>, concurrent_lookups: usize, cache: usize) -> Self {
        let tasks = TaskGroup::default();

        let (tx, receiver) = channel(cache);

        for i in 0..concurrent_lookups {
            let node = node.clone();
            let tx = tx.clone();
            tasks.spawn_with_name(format!("discv4 lookup #{}", i), {
                async move {
                    let node = node.clone();
                    let tx = tx.clone();
                    loop {
                        for record in node.lookup(rand::random()).await {
                            let _ = tx
                                .send(crate::sentry::devp2p::types::NodeRecord {
                                    addr: record.tcp_addr(),
                                    id: record.id,
                                })
                                .await;
                        }
                    }
                }
            });
        }

        Self { tasks, receiver }
    }
}

impl Stream for Discv4 {
    type Item = anyhow::Result<crate::sentry::devp2p::types::NodeRecord>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver)
            .poll_recv(cx)
            .map(|opt| opt.map(Ok))
    }
}
