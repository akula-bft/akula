use std::sync::Arc;

use super::{BlockCaches, Node, Sentry};
use crate::{
    kv::MdbxWithDirHandle,
    models::{BlockNumber, ChainConfig, H256, U256},
    p2p::types::Status,
};
use hashlink::LruCache;
use http::Uri;
use mdbx::EnvironmentKind;
use parking_lot::{Mutex, RwLock};
use tonic::transport::Channel;

#[derive(Debug)]
pub struct NodeBuilder<E>
where
    E: EnvironmentKind,
{
    sentries: Vec<Sentry>,
    env: Option<Arc<MdbxWithDirHandle<E>>>,
    config: Option<ChainConfig>,
    status: Option<Status>,
}

impl<E> Default for NodeBuilder<E>
where
    E: EnvironmentKind,
{
    fn default() -> Self {
        Self {
            sentries: Vec::new(),
            env: None,
            config: None,
            status: None,
        }
    }
}

impl<E> NodeBuilder<E>
where
    E: EnvironmentKind,
{
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

    pub fn set_env(mut self, env: Arc<MdbxWithDirHandle<E>>) -> Self {
        self.env = Some(env);
        self
    }

    pub fn build(self) -> anyhow::Result<Node<E>> {
        let env = self.env.ok_or_else(|| anyhow::anyhow!("env not set"))?;

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
            env,
            sentries,
            status,
            config,
            chain_tip: Default::default(),
            block_caches: Mutex::new(BlockCaches {
                bad_blocks: LruCache::new(1 << 12),
                parent_cache: LruCache::new(1 << 7),
                block_cache: LruCache::new(1 << 10),
            }),
            forks,
        })
    }
}
