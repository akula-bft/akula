use crate::downloader::chain_config::ChainConfig;
use anyhow;

pub struct ChainId(pub u32);

impl ChainId {
    pub fn from_config(config: &ChainConfig) -> anyhow::Result<Self> {
        Ok(ChainId(config.id))
    }
}
