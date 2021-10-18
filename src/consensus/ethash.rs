use super::{base::ConsensusEngineBase, *};
use crate::{chain::protocol_param::param, models::ChainConfig};
use async_trait::async_trait;

#[derive(Debug)]
pub struct Ethash {
    base: ConsensusEngineBase,
}

impl Ethash {
    pub fn new(chain_config: ChainConfig) -> Self {
        Self {
            base: ConsensusEngineBase::new(chain_config),
        }
    }
}

#[async_trait]
impl Consensus for Ethash {
    async fn pre_validate_block(&self, block: &Block, state: &mut dyn State) -> anyhow::Result<()> {
        self.base.pre_validate_block(block, state).await
    }
    async fn validate_block_header(
        &self,
        header: &BlockHeader,
        state: &mut dyn State,
        with_future_timestamp_check: bool,
    ) -> anyhow::Result<()> {
        self.base
            .validate_block_header(header, state, with_future_timestamp_check)
            .await
    }
    async fn validate_seal(&self, header: &BlockHeader) -> anyhow::Result<()> {
        // TOD: Ethash stuff here
        let _ = header;
        Ok(())
    }
    async fn finalize(
        &self,
        header: &PartialHeader,
        ommers: &[BlockHeader],
        revision: Revision,
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        let mut changes = Vec::with_capacity(1 + ommers.len());
        let block_reward = {
            if revision >= Revision::Constantinople {
                *param::BLOCK_REWARD_CONSTANTINOPLE
            } else if revision >= Revision::Byzantium {
                *param::BLOCK_REWARD_BYZANTIUM
            } else {
                *param::BLOCK_REWARD_FRONTIER
            }
        };

        let block_number = header.number;
        let mut miner_reward = block_reward;
        for ommer in ommers {
            let ommer_reward =
                (U256::from(8 + ommer.number.0 - block_number.0) * block_reward) >> 3;
            changes.push(FinalizationChange::Reward {
                address: ommer.beneficiary,
                amount: ommer_reward,
            });
            miner_reward += block_reward / 32;
        }

        changes.push(FinalizationChange::Reward {
            address: header.beneficiary,
            amount: miner_reward,
        });

        Ok(changes)
    }

    async fn get_beneficiary(&self, header: &BlockHeader) -> anyhow::Result<Address> {
        Ok(header.beneficiary)
    }
}
