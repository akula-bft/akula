use self::difficulty::BlockDifficultyBombData;
use super::{base::ConsensusEngineBase, *};
use crate::{h256_to_u256, BlockReader};
use ::ethash::LightDAG;
use lru::LruCache;
use parking_lot::Mutex;
use std::sync::Arc;

pub mod difficulty;
pub mod fork_choice_graph;

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
    duration_limit: u64,
    block_reward: BlockRewardSchedule,
    homestead_formula: Option<BlockNumber>,
    byzantium_formula: Option<BlockNumber>,
    difficulty_bomb: Option<DifficultyBomb>,
    skip_pow_verification: bool,

    dag_cache: DagCache,
    fork_choice_graph: Arc<Mutex<ForkChoiceGraph>>,
}

impl Ethash {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        chain_id: ChainId,
        eip1559_block: Option<BlockNumber>,
        duration_limit: u64,
        block_reward: BlockRewardSchedule,
        homestead_formula: Option<BlockNumber>,
        byzantium_formula: Option<BlockNumber>,
        difficulty_bomb: Option<DifficultyBomb>,
        skip_pow_verification: bool,
    ) -> Self {
        Self {
            base: ConsensusEngineBase::new(chain_id, eip1559_block, Some(32)),
            dag_cache: DagCache::new(),

            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_formula,
            difficulty_bomb,
            skip_pow_verification,

            fork_choice_graph: Arc::new(Mutex::new(ForkChoiceGraph::new())),
        }
    }
}

impl Consensus for Ethash {
    fn name(&self) -> &str {
        "Ethash"
    }

    fn fork_choice_mode(&self) -> ForkChoiceMode {
        ForkChoiceMode::Difficulty(self.fork_choice_graph.clone())
    }

    fn pre_validate_block(&self, block: &Block, state: &dyn BlockReader) -> Result<(), DuoError> {
        self.base.pre_validate_block(block)?;

        if block.ommers.len() > 2 {
            return Err(ValidationError::TooManyOmmers.into());
        }

        if block.ommers.len() == 2 && block.ommers[0] == block.ommers[1] {
            return Err(ValidationError::DuplicateOmmer.into());
        }

        let parent =
            state
                .read_parent_header(&block.header)?
                .ok_or(ValidationError::UnknownParent {
                    number: block.header.number,
                    parent_hash: block.header.parent_hash,
                })?;

        for ommer in &block.ommers {
            let ommer_parent =
                state
                    .read_parent_header(ommer)?
                    .ok_or(ValidationError::OmmerUnknownParent {
                        number: ommer.number,
                        parent_hash: ommer.parent_hash,
                    })?;

            self.base
                .validate_block_header(ommer, &ommer_parent, false)
                .map_err(|e| match e {
                    DuoError::Internal(e) => DuoError::Internal(e),
                    DuoError::Validation(e) => {
                        DuoError::Validation(ValidationError::InvalidOmmerHeader {
                            inner: Box::new(e),
                        })
                    }
                })?;
            let mut old_ommers = vec![];
            if !self.base.is_kin(
                ommer,
                &parent,
                block.header.parent_hash,
                6,
                state,
                &mut old_ommers,
            )? {
                return Err(ValidationError::NotAnOmmer.into());
            }
            for oo in old_ommers {
                if oo == *ommer {
                    return Err(ValidationError::DuplicateOmmer.into());
                }
            }
        }

        Ok(())
    }

    fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), DuoError> {
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
        header: &BlockHeader,
        ommers: &[BlockHeader],
    ) -> anyhow::Result<Vec<FinalizationChange>> {
        let mut changes: Vec<FinalizationChange> = Vec::with_capacity(1 + ommers.len());

        let block_number = header.number;
        let block_reward = self.block_reward.for_block(block_number);

        Ok(if block_reward > 0 {
            let mut changes = Vec::with_capacity(1 + ommers.len());
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
                amount: miner_reward,
                ommer: false,
            });

            changes
        } else {
            vec![]
        })
    }

    fn parlia(&mut self) -> Option<&mut Parlia> {
        None
    }
}
