use crate::downloader::chain_config::ChainConfig;
use anyhow;
use ethereum_types::H256;
use hex;
use std::collections::BTreeSet;

pub struct ChainForkConfig {
    pub genesis_block_hash: H256,
    // BTreeSet ensures uniqueness and sort order
    pub fork_block_numbers: BTreeSet<u64>,
}

fn hash_from_str(hash_str: &str) -> anyhow::Result<H256> {
    Ok(H256::from_slice(&hex::decode(hash_str)?))
}

impl ChainForkConfig {
    pub fn from_config(config: &ChainConfig) -> anyhow::Result<Self> {
        return Ok(ChainForkConfig {
            genesis_block_hash: hash_from_str(&config.genesis)?,
            fork_block_numbers: config
                .fork_blocks
                .iter()
                .cloned()
                .collect::<BTreeSet<u64>>(),
        });
    }
}
