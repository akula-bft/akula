use self::difficulty::BlockDifficultyBombData;
use super::{base::ConsensusEngineBase, *};
use crate::{chain::protocol_param::param, h256_to_u256};
use ::ethash::LightDAG;
use lru::LruCache;
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};

pub mod difficulty;

type Dag = LightDAG;

#[derive(Debug)]
struct DagCache {
    inner: Mutex<LruCache<u64, Arc<Dag>>>,
}

impl DagCache {
    fn new() -> Self {
        Self {
            inner: Mutex::new(LruCache::new(16)),
        }
    }

    fn get(&self, block_number: BlockNumber) -> Arc<Dag> {
        let epoch = block_number.0 / 30_000;

        let mut dag_cache = self.inner.lock();

        dag_cache.get(&epoch).cloned().unwrap_or_else(|| {
            let dag = Arc::new(Dag::new(block_number.0.into()));

            dag_cache.put(epoch, dag.clone());

            dag
        })
    }
}

#[derive(Debug)]
pub struct Ethash {
    base: ConsensusEngineBase,
    dag_cache: DagCache,
    duration_limit: u64,
    block_reward: BTreeMap<BlockNumber, U256>,
    homestead_formula: Option<BlockNumber>,
    byzantium_formula: Option<BlockNumber>,
    difficulty_bomb: Option<DifficultyBomb>,
    skip_pow_verification: bool,
}

impl Ethash {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        eip1559_block: Option<BlockNumber>,
        duration_limit: u64,
        block_reward: BTreeMap<BlockNumber, U256>,
        homestead_formula: Option<BlockNumber>,
        byzantium_formula: Option<BlockNumber>,
        difficulty_bomb: Option<DifficultyBomb>,
        skip_pow_verification: bool,
    ) -> Self {
        Self {
            base: ConsensusEngineBase::new(chain_id, eip1559_block),
            dag_cache: DagCache::new(),
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_formula,
            difficulty_bomb,
            skip_pow_verification,
        }
    }
}

impl Consensus for Ethash {
    fn pre_validate_block(&self, block: &Block, state: &mut dyn State) -> anyhow::Result<()> {
        self.base.pre_validate_block(block, state)
    }

    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> anyhow::Result<()> {
        self.base
            .validate_block_header(header, parent, with_future_timestamp_check)?;

        let parent_has_uncles = parent.ommers_hash != EMPTY_LIST_HASH;
        let difficulty = difficulty::canonical_difficulty(
            header.number,
            header.timestamp,
            parent.difficulty,
            parent.timestamp,
            parent_has_uncles,
            switch_is_active(self.byzantium_formula, header.number),
            switch_is_active(self.homestead_formula, header.number),
            self.difficulty_bomb
                .as_ref()
                .map(|b| BlockDifficultyBombData {
                    delay_to: b.get_delay_to(header.number),
                }),
        );
        if difficulty != header.difficulty {
            return Err(ValidationError::WrongDifficulty.into());
        }

        if !self.skip_pow_verification {
            let light_dag = self.dag_cache.get(header.number);
            let (mixh, final_hash) = light_dag.hashimoto(header.truncated_hash(), header.nonce);

            if mixh != header.mix_hash {
                return Err(ValidationError::InvalidSeal.into());
            }

            if h256_to_u256(final_hash) > ::ethash::cross_boundary(header.difficulty) {
                return Err(ValidationError::InvalidSeal.into());
            }
        }
        Ok(())
    }
    fn finalize(
        &self,
        header: &PartialHeader,
        ommers: &[BlockHeader],
        revision: Revision,
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        let mut changes = Vec::with_capacity(1 + ommers.len());
        let block_reward = {
            if revision >= Revision::Constantinople {
                param::BLOCK_REWARD_CONSTANTINOPLE
            } else if revision >= Revision::Byzantium {
                param::BLOCK_REWARD_BYZANTIUM
            } else {
                param::BLOCK_REWARD_FRONTIER
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
                ommer: true,
            });
            miner_reward += block_reward / 32;
        }

        changes.push(FinalizationChange::Reward {
            address: header.beneficiary,
            amount: miner_reward.into(),
            ommer: false,
        });

        Ok(changes)
    }

    fn get_beneficiary(&self, header: &BlockHeader) -> anyhow::Result<Address> {
        Ok(header.beneficiary)
    }
}
