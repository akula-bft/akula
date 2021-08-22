use ethereum_types::*;
use evmodin::Revision;
use serde::Deserialize;
use std::collections::{BTreeSet, HashSet};

#[derive(Debug, Deserialize)]
pub struct DaoConfig {
    pub block_number: u64,
    pub drain: HashSet<Address>,
    pub beneficiary: Address,
}

#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainConfig {
    pub chain_id: u64,
    pub homestead_block: Option<u64>,
    pub dao_fork: Option<DaoConfig>,
    pub tangerine_block: Option<u64>,
    pub spurious_block: Option<u64>,
    pub byzantium_block: Option<u64>,
    pub constantinople_block: Option<u64>,
    pub petersburg_block: Option<u64>,
    pub istanbul_block: Option<u64>,
    pub muir_glacier_block: Option<u64>,
    pub berlin_block: Option<u64>,
    pub london_block: Option<u64>,
}

impl ChainConfig {
    pub fn gather_forks(&self) -> BTreeSet<u64> {
        [
            self.homestead_block,
            self.dao_fork.as_ref().map(|c| c.block_number),
            self.tangerine_block,
            self.spurious_block,
            self.byzantium_block,
            self.constantinople_block,
            self.petersburg_block,
            self.istanbul_block,
            self.muir_glacier_block,
            self.berlin_block,
            self.london_block,
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

    pub fn revision(&self, block_num: u64) -> Revision {
        for (fork, revision) in [
            (self.london_block, Revision::London),
            (self.berlin_block, Revision::Berlin),
            (self.istanbul_block, Revision::Istanbul),
            (self.petersburg_block, Revision::Petersburg),
            (self.constantinople_block, Revision::Constantinople),
            (self.byzantium_block, Revision::Byzantium),
            (self.spurious_block, Revision::Spurious),
            (self.tangerine_block, Revision::Tangerine),
            (self.homestead_block, Revision::Homestead),
        ] {
            if let Some(fork_block) = fork {
                if block_num >= fork_block {
                    return revision;
                }
            }
        }

        Revision::Frontier
    }

    pub fn is_dao_block(&self, block_num: u64) -> bool {
        self.dao_fork
            .as_ref()
            .map(|c| c.block_number == block_num)
            .unwrap_or(false)
    }
}
