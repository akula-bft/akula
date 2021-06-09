use crate::downloader::chain_config::ChainConfig;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(transparent)]
pub struct ChainId(pub u32);

impl ChainId {
    pub fn from_config(config: &ChainConfig) -> anyhow::Result<Self> {
        Ok(ChainId(config.id))
    }
}
