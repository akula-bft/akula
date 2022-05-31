use crate::{
    genesis::GenesisState,
    models::{BlockNumber, ChainSpec, NetworkId, H256},
    res::chainspec,
};
use anyhow::anyhow;

const REPOSITORY_URL: &str = "https://github.com/akula-bft/akula";

#[derive(Debug, Clone)]
pub struct ChainConfig {
    pub chain_spec: ChainSpec,
    pub genesis_hash: H256,
}

impl From<ChainSpec> for ChainConfig {
    fn from(chain_spec: ChainSpec) -> Self {
        let genesis = GenesisState::new(chain_spec.clone());
        Self {
            chain_spec,
            genesis_hash: genesis.header(&genesis.initial_state()).hash(),
        }
    }
}

impl ChainConfig {
    pub fn new(name: &str) -> anyhow::Result<Self> {
        match name.to_lowercase().as_ref() {
            "mainnet" | "ethereum" => Ok(ChainConfig::from(chainspec::MAINNET.clone())),
            "ropsten" => Ok(ChainConfig::from(chainspec::ROPSTEN.clone())),
            "rinkeby" => Ok(ChainConfig::from(chainspec::RINKEBY.clone())),
            "goerli" => Ok(ChainConfig::from(chainspec::GOERLI.clone())),
            "sepolia" => Ok(ChainConfig::from(chainspec::SEPOLIA.clone())),
            _ => Err(anyhow!(
                "{name} is not yet supported, please fill an issue at {REPOSITORY_URL} and we'll maybe add support for it in the foreseeable future",
            )),
        }
    }

    pub const fn network_id(&self) -> NetworkId {
        self.chain_spec.params.network_id
    }

    pub fn chain_name(&self) -> &str {
        &self.chain_spec.name
    }

    pub fn forks(&self) -> Vec<BlockNumber> {
        self.chain_spec
            .gather_forks()
            .into_iter()
            .collect::<Vec<_>>()
    }

    pub fn bootnodes(&self) -> Vec<String> {
        self.chain_spec.p2p.bootnodes.clone()
    }

    pub fn dns(&self) -> Option<String> {
        self.chain_spec.p2p.dns.clone()
    }
}
