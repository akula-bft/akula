use super::BlockNumber;
use educe::Educe;
use ethereum_types::*;
use evmodin::Revision;
use serde::*;
use std::collections::{BTreeSet, HashSet};

#[derive(Clone, Copy, Debug, Educe, PartialEq, Serialize, Deserialize)]
#[educe(Default)]
pub enum SealEngineType {
    #[educe(Default)]
    NoProof,
    Ethash,
    Clique,
    AuRa,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaoConfig {
    pub block_number: BlockNumber,
    pub drain: HashSet<Address>,
    pub beneficiary: Address,
}

#[allow(non_snake_case)]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChainConfig {
    pub seal_engine: SealEngineType,
    pub chain_id: u64,
    pub homestead_block: Option<BlockNumber>,
    pub dao_fork: Option<DaoConfig>,
    pub tangerine_block: Option<BlockNumber>,
    pub spurious_block: Option<BlockNumber>,
    pub byzantium_block: Option<BlockNumber>,
    pub constantinople_block: Option<BlockNumber>,
    pub petersburg_block: Option<BlockNumber>,
    pub istanbul_block: Option<BlockNumber>,
    pub muir_glacier_block: Option<BlockNumber>,
    pub berlin_block: Option<BlockNumber>,
    pub london_block: Option<BlockNumber>,
    pub arrow_glacier_block: Option<BlockNumber>,
}

impl ChainConfig {
    pub fn gather_forks(&self) -> BTreeSet<BlockNumber> {
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
            self.arrow_glacier_block,
        ]
        .iter()
        .filter_map(|b| {
            if let Some(b) = *b {
                if b.0 > 0 {
                    return Some(b);
                }
            }

            None
        })
        .collect()
    }

    pub fn revision(&self, block_number: impl Into<BlockNumber>) -> Revision {
        let block_number = block_number.into();
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
                if block_number >= fork_block {
                    return revision;
                }
            }
        }

        Revision::Frontier
    }

    pub fn is_dao_block(&self, block_number: impl Into<BlockNumber>) -> bool {
        self.dao_fork
            .as_ref()
            .map(|c| c.block_number == block_number.into())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_block_numbers() {
        assert_eq!(
            crate::res::genesis::MAINNET.config.gather_forks(),
            vec![
                1_150_000, 1_920_000, 2_463_000, 2_675_000, 4_370_000, 7_280_000, 9_069_000,
                9_200_000, 12_244_000, 12_965_000, 13_773_000
            ]
            .into_iter()
            .map(BlockNumber)
            .collect()
        );
    }
}
