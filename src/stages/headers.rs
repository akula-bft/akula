use hashbrown::HashMap;
use tokio_stream::StreamExt;

use crate::{
    consensus::Consensus,
    models::{BlockHeader, BlockNumber, H256},
    p2p::{collections::Graph, node::Node, types::HeaderRequest},
};
use std::{sync::Arc, time::Duration};
use tokio::pin;
use tracing::*;

const HEADERS_UPPER_BOUND: usize = 1 << 10;
const REQUEST_INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownload {
    pub node: Arc<Node>,
    pub consensus: Arc<dyn Consensus>,
    pub max_block: BlockNumber,
    pub graph: Graph,
}

impl HeaderDownload {
    const BACK_OFF: Duration = Duration::from_secs(2);
    const ANCHOR_TRESHOLD: usize = 8;

    async fn put_anchors(
        &mut self,
        hash: H256,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let limit = (*end - *start) / HEADERS_UPPER_BOUND as u64 + 1;
        if limit > HEADERS_UPPER_BOUND as u64 {
            anyhow::bail!("too many headers to download")
        }
        let anchor_request = HeaderRequest {
            start: start.into(),
            limit,
            skip: HEADERS_UPPER_BOUND as u64 - 1,
            reverse: false,
        };

        let stream = self.node.stream_headers().await.timeout(Self::BACK_OFF);
        pin!(stream);

        let anchors = 'outer_loop: loop {
            match stream.next().await {
                Some(Ok(msg)) => match msg.msg {
                    crate::p2p::types::Message::BlockHeaders(inner) => {
                        info!("{:?}", &inner);
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
                    _ => continue 'outer_loop,
                },

                _ => {
                    self.node.send_header_request(anchor_request).await?;
                }
            }
        };

        Ok(anchors)
    }

    pub async fn download_headers(
        &mut self,
        hash: H256,
        start: BlockNumber,
        end: BlockNumber,
    ) -> anyhow::Result<()> {
        let anchors = self.put_anchors(hash, start, end).await?;
        let mut requests = HashMap::<BlockNumber, HeaderRequest>::with_capacity(
            anchors.len() * HEADERS_UPPER_BOUND,
        );

        for anchor in anchors {
            let k = anchor.number.into();
            let request = HeaderRequest {
                start: k,
                reverse: true,
                ..Default::default()
            };

            requests.insert(anchor.number, request);
            self.graph.insert(anchor);
        }

        let stream = self.node.stream_headers().await.timeout(Self::BACK_OFF);
        pin!(stream);

        while !requests.iter().all(|(k, _)| self.graph.contains(k)) {
            match stream.next().await {
                Some(Ok(msg)) => match msg.msg {
                    crate::p2p::types::Message::BlockHeaders(inner) => {
                        if inner.headers.is_empty() {
                            self.node.penalize_peer(msg.peer_id).await?;
                            continue;
                        }

                        if requests.contains_key(&(inner.headers[0].number.into())) {
                            self.graph.extend(inner.headers);
                            info!("Received headers: {}", self.graph.len());
                        }
                    }
                    _ => continue,
                },
                _ => {
                    self.node
                        .clone()
                        .send_many_header_requests(
                            requests
                                .iter()
                                .filter(|(n, _)| !self.graph.contains(n))
                                .map(|(_, &v)| v),
                        )
                        .await?;
                }
            }
        }

        let h = self.graph.dfs().expect("unreachable");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::HeaderDownload;
    use crate::{
        akula_tracing::{self, Component},
        consensus::{engine_factory, Consensus},
        models::{BlockNumber, ChainConfig},
        p2p::{collections::Graph, node::NodeBuilder},
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
                graph: Graph::new(),
            };
            let chain_config = ChainConfig::new("mainnet").unwrap();
        });
    }
}
