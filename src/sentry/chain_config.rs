use super::chain_id::ChainId;
use crate::{
    genesis::GenesisState,
    models::{BlockNumber, ChainSpec},
};
use std::collections::HashMap;

pub struct ChainsConfig(HashMap<String, ChainConfig>);

#[derive(Clone)]
pub struct ChainConfig {
    chain_spec: ChainSpec,
    genesis_block_hash: ethereum_types::H256,
}

impl ChainConfig {
    fn new(chain_spec: ChainSpec) -> Self {
        let genesis = GenesisState::new(chain_spec.clone());
        let genesis_header = genesis.header(&genesis.initial_state());
        let genesis_block_hash = genesis_header.hash();

        Self {
            chain_spec,
            genesis_block_hash,
        }
    }

    pub fn id(&self) -> ChainId {
        self.chain_spec.params.chain_id
    }

    pub fn genesis_block_hash(&self) -> ethereum_types::H256 {
        self.genesis_block_hash
    }

    pub fn fork_block_numbers(&self) -> Vec<BlockNumber> {
        self.chain_spec.gather_forks().iter().cloned().collect()
    }
}

impl ChainsConfig {
    pub fn new() -> anyhow::Result<Self> {
        let mut configs = HashMap::<String, ChainConfig>::new();
        configs.insert(
            String::from("mainnet"),
            ChainConfig::new(crate::res::chainspec::MAINNET.clone()),
        );
        configs.insert(
            String::from("ropsten"),
            ChainConfig::new(crate::res::chainspec::ROPSTEN.clone()),
        );
        configs.insert(
            String::from("rinkeby"),
            ChainConfig::new(crate::res::chainspec::RINKEBY.clone()),
        );
        Ok(ChainsConfig(configs))
    }

    pub fn get(&self, chain_name: &str) -> Option<&ChainConfig> {
        self.0.get(chain_name)
    }
}

impl ChainsConfig {
    pub fn chain_names(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect::<Vec<&str>>()
    }
}
