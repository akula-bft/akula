use super::chain_id::ChainId;
use serde::Deserialize;
use std::collections::HashMap;

pub struct ChainsConfig(pub HashMap<String, ChainConfig>);

#[derive(Deserialize, Clone)]
pub struct ChainConfig {
    pub id: ChainId,
    #[serde(rename = "genesis")]
    pub genesis_block_hash: ethereum_types::H256,
    #[serde(rename = "fork_blocks")]
    pub fork_block_numbers: Vec<u64>,
}

impl ChainsConfig {
    pub fn new() -> anyhow::Result<Self> {
        let config_text = include_str!("chain_config.toml");
        let configs: HashMap<String, ChainConfig> = toml::from_str(config_text)?;
        Ok(ChainsConfig(configs))
    }
}

impl ChainsConfig {
    pub fn chain_names(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect::<Vec<&str>>()
    }
}
