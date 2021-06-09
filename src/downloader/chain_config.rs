use anyhow;
use serde::Deserialize;
use std::collections::HashMap;
use toml;

pub struct ChainsConfig(pub HashMap<String, ChainConfig>);

#[derive(Hash, Deserialize)]
pub struct ChainConfig {
    pub id: u32,
    pub genesis: String,
    pub fork_blocks: Vec<u64>,
}

impl ChainsConfig {
    pub fn new() -> anyhow::Result<Self> {
        let config_text = include_str!("chain_config.toml");
        let configs: HashMap<String, ChainConfig> = toml::from_str(config_text)?;
        Ok(ChainsConfig(configs))
    }
}
