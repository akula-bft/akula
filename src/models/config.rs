use ethereum_types::H256;
use serde::Deserialize;
use std::collections::BTreeSet;

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainConfig {
    chain_id: Option<u64>,
    homestead_block: Option<u64>,
    dao_fork_block: Option<u64>,
    dao_fork_support: bool,
    eip_150_block: Option<u64>,
    eip_150_hash: Option<H256>,
    eip_155_block: Option<u64>,
    eip_158_block: Option<u64>,
    byzantium_block: Option<u64>,
    constantinople_block: Option<u64>,
    petersburg_block: Option<u64>,
    istanbul_block: Option<u64>,
    muir_glacier_block: Option<u64>,
    yoloV2_block: Option<u64>,
    ewasm_block: Option<u64>,
}

impl ChainConfig {
    pub fn gather_forks(&self) -> BTreeSet<u64> {
        [
            self.homestead_block,
            self.dao_fork_block,
            self.eip_150_block,
            self.eip_155_block,
            self.eip_158_block,
            self.byzantium_block,
            self.constantinople_block,
            self.petersburg_block,
            self.istanbul_block,
            self.muir_glacier_block,
            self.yoloV2_block,
            self.ewasm_block,
        ]
        .iter()
        .filter_map(|b| {
            if let Some(b) = *b {
                if b > 0 {
                    return Some(b);
                }
            }

            None
        })
        .collect()
    }
}
