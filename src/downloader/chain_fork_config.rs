use crate::downloader::chain_config::ChainConfig;
use ethereum_types::H256;
use std::collections::BTreeSet;

pub struct ChainForkConfig {
    pub genesis_block_hash: H256,
    // BTreeSet ensures uniqueness and sort order
    pub fork_block_numbers: BTreeSet<u64>,
}

impl ChainForkConfig {
    pub fn from_config(config: &ChainConfig) -> anyhow::Result<Self> {
        return Ok(ChainForkConfig {
            genesis_block_hash: config.genesis,
            fork_block_numbers: config
                .fork_blocks
                .iter()
                .cloned()
                .collect::<BTreeSet<u64>>(),
        });
    }
}
