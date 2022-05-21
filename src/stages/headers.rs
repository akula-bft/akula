#![allow(unused_imports)]

use crate::{
    consensus::Consensus,
    kv::{mdbx::MdbxTransaction, tables},
    models::{BlockHeader, BlockNumber, H256},
    p2p::{
        node::Node,
        types::{BlockId, HeaderRequest, InboundMessage, Message, Status},
    },
    stagedsync::{
        stage::{ExecOutput, Stage, StageError, StageInput, UnwindInput, UnwindOutput},
        stages::HEADERS,
    },
};

use anyhow::format_err;
use async_trait::async_trait;
use ethnum::U256;
use futures::Stream;
use hashbrown::{HashMap, HashSet};
use hashlink::LinkedHashMap;
use mdbx::{EnvironmentKind, RW};
use rayon::{
    iter::{
        IndexedParallelIterator, IntoParallelRefIterator, ParallelDrainRange, ParallelIterator,
    },
    slice::ParallelSliceMut,
};
use std::{
    collections::{BinaryHeap, VecDeque},
    hash::Hash,
    pin::Pin,
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    pin,
    time::{error::Elapsed, Timeout},
};
use tokio_stream::StreamExt;
use tracing::*;

const HEADERS_UPPER_BOUND: usize = 1 << 10;
const STAGE_UPPER_BOUND: usize = 3 << 15;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

impl Ord for Link {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.height.cmp(&other.height).reverse()
    }
}

impl PartialOrd for Link {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Link {
    height: BlockNumber,
    hash: H256,
    parent_hash: H256,
}

#[derive(Debug)]
pub struct Graph {
    skip_list: HashMap<H256, HashSet<H256>>,
    raw: HashMap<H256, BlockHeader>,
    q: Vec<Link>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            skip_list: HashMap::new(),
            raw: HashMap::new(),
            q: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.raw.len()
    }

    pub fn insert(&mut self, header: BlockHeader) {
        let hash = header.hash();
        if self.raw.contains_key(&hash) {
            return;
        }

        let link = Link {
            height: header.number,
            hash,
            parent_hash: header.parent_hash,
        };
        self.skip_list
            .entry(header.parent_hash)
            .or_default()
            .insert(hash);
        self.raw.insert(hash, header);
        self.q.push(link);
    }

    pub fn dfs(&mut self) -> Option<H256> {
        let mut roots = Vec::new();
        for node in self.q.drain(..) {
            // Check if we don't have node's parent, and if it's the case - node is root.
            if !self.raw.contains_key(&node.parent_hash) {
                roots.push(node)
            }
        }
        if roots.is_empty() {
            return None;
        }

        let mut stack = VecDeque::new();
        for root in roots {
            stack.push_back(root.hash);
        }

        let mut branches = HashSet::new();

        // DFS and then backtrack.
        while let Some(hash) = stack.pop_back() {
            if let Some(next) = self.skip_list.remove(&hash) {
                for child in next {
                    stack.push_back(child);
                }
            } else {
                branches.insert(hash);
            }
        }

        if branches.len() > 1 {
            let mut tds = HashMap::new();
            for branch in branches {
                let mut td = U256::ZERO;
                let mut parent = branch;
                while let Some(header) = self.raw.get(&parent) {
                    td = td + header.difficulty;
                    parent = header.parent_hash;
                }
                tds.insert(branch, td);
            }

            tds.iter().max_by_key(|(_, td)| *td).map(|(hash, _)| *hash)
        } else {
            branches.into_iter().next()
        }
    }

    pub fn collect(self, tail: &H256) -> Vec<BlockHeader> {
        let mut raw = self.raw;
        let mut headers = Vec::new();
        let mut current = *tail;
        while let Some(header) = raw.remove(&current) {
            current = header.parent_hash;
            headers.push(header);
        }
        headers.reverse();
        headers
    }
}

impl Extend<BlockHeader> for Graph {
    fn extend<T: IntoIterator<Item = BlockHeader>>(&mut self, iter: T) {
        for header in iter {
            self.insert(header);
        }
    }
}

#[derive(Debug)]
pub struct HeaderDownload {
    pub node: Arc<Node>,
    /// Consensus engine used.
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
}

impl HeaderDownload {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let anchors = self
            .prepare_anchors(BlockNumber(1), BlockNumber(98_305))
            .await?;

        let mut graph = Graph::new();
        // Mapping from the anchor hash to the corresponding request.
        let mut header_requests = HashMap::<H256, HeaderRequest>::new();
        for anchor in anchors {
            let hash = anchor.hash();
            let request = HeaderRequest {
                start: hash.into(),
                reverse: true,
                ..Default::default()
            };
            graph.insert(anchor);
            header_requests.insert(hash, request);
        }

        let stream = self
            .node
            .stream_headers()
            .await
            .timeout(Duration::from_secs(2));
        pin!(stream);

        while !header_requests.is_empty() {
            match stream.next().await {
                Some(Ok(msg)) => match msg.msg {
                    Message::BlockHeaders(inner) => {
                        if inner.headers.is_empty() {
                            self.node.penalize_peer(msg.peer_id).await?;
                            continue;
                        }

                        if header_requests
                            .remove(&inner.headers.first().unwrap().hash())
                            .is_some()
                        {
                            graph.extend(inner.headers);
                        }
                    }
                    _ => continue,
                },
                _ => {
                    self.node
                        .clone()
                        .send_many_header_requests(header_requests.values().copied())
                        .await?;
                }
            }
        }

        if let Some(headers) = match graph.dfs() {
            Some(tail) => Some(graph.collect(&tail)),
            _ => None,
        } {
            for header in headers {
                debug!("Header: {}|{:?}", header.number, header.hash());
            }
        }

        Ok(())
    }

    pub async fn prepare_anchors(
        &mut self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let stream = self
            .node
            .stream_headers()
            .await
            .timeout(Duration::from_secs(2));
        pin!(stream);

        let limit = (*end - *start) / HEADERS_UPPER_BOUND as u64 + 1;
        let anchor_request = HeaderRequest {
            start: start.into(),
            skip: HEADERS_UPPER_BOUND as u64 - 1,
            limit,
            reverse: false,
        };
        let anchors = 'outer_loop: loop {
            match stream.next().await {
                Some(Ok(msg)) => match msg.msg {
                    Message::BlockHeaders(inner) => {
                        if inner.headers.is_empty() {
                            self.node.penalize_peer(msg.peer_id).await?;
                            continue 'outer_loop;
                        }

                        let mut cur = start;
                        for header in &inner.headers {
                            if header.number != cur && limit > 1 {
                                self.node.penalize_peer(msg.peer_id).await?;
                                continue 'outer_loop;
                            }
                            cur += HEADERS_UPPER_BOUND;
                        }
                        break inner.headers;
                    }
                    _ => continue,
                },
                _ => {
                    self.node.send_header_request(anchor_request).await?;
                }
            }
        };

        Ok(anchors)
    }
}

#[cfg(test)]
mod tests {
    use super::HeaderDownload;
    use crate::{
        akula_tracing::{self, Component},
        consensus::{engine_factory, Consensus},
        models::{BlockNumber, ChainConfig},
        p2p::node::NodeBuilder,
    };
    use http::Uri;
    use std::sync::Arc;
    use tracing_subscriber::util::SubscriberInitExt;

    #[test]
    fn test_header_download() {
        akula_tracing::build_subscriber(Component::Core).init();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        runtime.block_on(async move {
            let node = Arc::new(
                NodeBuilder::default()
                    .add_sentry("http://127.0.0.1:8080".parse::<Uri>().unwrap())
                    .build()
                    .unwrap(),
            );

            let consensus: Arc<dyn Consensus> =
                engine_factory(ChainConfig::new("mainnet").unwrap().chain_spec)
                    .unwrap()
                    .into();
            let mut down = HeaderDownload {
                node,
                consensus,
                max_block: BlockNumber(0),
            };

            down.run().await.unwrap();
        });
    }
}
