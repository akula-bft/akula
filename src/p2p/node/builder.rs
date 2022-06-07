use super::{stash::Stash, BlockCaches, Node, Sentry};
use crate::{
    models::{BlockNumber, ChainConfig, H256, U256},
    p2p::types::Status,
};
use hashlink::LruCache;
use http::Uri;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use tonic::transport::Channel;

#[derive(Debug, Default)]
pub struct NodeBuilder {
    sentries: Vec<Sentry>,
    stash: Option<Arc<dyn Stash>>,
    config: Option<ChainConfig>,
    status: Option<Status>,
}

impl NodeBuilder {
    pub fn add_sentry(mut self, endpoint: impl Into<Uri>) -> Self {
        self.sentries.push(Sentry::new(
            Channel::builder(endpoint.into()).connect_lazy(),
        ));
        self
    }

    pub fn set_config(mut self, config: ChainConfig) -> Self {
        self.config = Some(config);
        self
    }
    pub fn set_chain_head(mut self, height: BlockNumber, hash: H256, td: U256) -> Self {
        let status = Status {
            height,
            hash,
            total_difficulty: H256::from(td.to_be_bytes()),
        };
        self.status = Some(status);
        self
    }

    pub fn set_stash(mut self, stash: Arc<dyn Stash>) -> Self {
        self.stash = Some(stash);
        self
    }

    pub fn build(self) -> anyhow::Result<Node> {
        let stash = self.stash.unwrap_or_else(|| Arc::new(()));
        let sentries = self.sentries;
        if sentries.is_empty() {
            anyhow::bail!("No sentries");
        }

        let config = self
            .config
            .unwrap_or_else(|| ChainConfig::new("mainnet").unwrap());
        let status = RwLock::new(self.status.unwrap_or_else(|| Status::from(&config)));
        let forks = config.forks().into_iter().map(|f| *f).collect::<Vec<_>>();

        Ok(Node {
            stash,
            sentries,
            status,
            config,
            chain_tip: Default::default(),
            bad_blocks: Default::default(),
            block_caches: Mutex::new(BlockCaches {
                parent_cache: LruCache::new(1 << 7),
                block_cache: LruCache::new(1 << 10),
            }),
            forks,
        })
    }
}
