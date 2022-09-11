use super::*;
use crate::{chain::protocol_param::param, models::*, state::*, trie::root_hash};
use std::{collections::BTreeMap, time::SystemTime};

pub const MIN_GAS_LIMIT: u64 = 5000;

#[derive(Debug)]
pub struct ConsensusEngineBase {
    chain_id: ChainId,
    eip1559_block: Option<BlockNumber>,
    max_extra_data_length: Option<usize>,
}

impl ConsensusEngineBase {
    pub fn new(
        chain_id: ChainId,
        eip1559_block: Option<BlockNumber>,
        max_extra_data_length: Option<usize>,
    ) -> Self {
        Self {
            chain_id,
            eip1559_block,
            max_extra_data_length,
        }
    }

    pub fn validate_block_header(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
        with_future_timestamp_check: bool,
    ) -> Result<(), DuoError> {
        if with_future_timestamp_check {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            if header.timestamp > now {
                return Err(ValidationError::FutureBlock {
                    now,
                    got: header.timestamp,
                }
                .into());
            }
        }

        if header.gas_used > header.gas_limit {
            return Err(ValidationError::GasAboveLimit {
                used: header.gas_used,
                limit: header.gas_limit,
            }
            .into());
        }

        if header.gas_limit < MIN_GAS_LIMIT {
            return Err(ValidationError::InvalidGasLimit.into());
        }

        // https://github.com/ethereum/go-ethereum/blob/v1.9.25/consensus/ethash/consensus.go#L267
        // https://eips.ethereum.org/EIPS/eip-1985
        if header.gas_limit > i64::MAX.try_into().unwrap() {
            return Err(ValidationError::InvalidGasLimit.into());
        }

        if let Some(limit) = self.max_extra_data_length {
            if header.extra_data.len() > limit {
                return Err(ValidationError::ExtraDataTooLong.into());
            }
        }

        if header.timestamp < parent.timestamp {
            return Err(ValidationError::InvalidTimestamp {
                parent: parent.timestamp,
                current: header.timestamp,
            }
            .into());
        }

        let mut parent_gas_limit = parent.gas_limit;
        if let Some(fork_block) = self.eip1559_block {
            if fork_block == header.number {
                parent_gas_limit = parent.gas_limit * param::ELASTICITY_MULTIPLIER;
            }
        }

        let gas_delta = if header.gas_limit > parent_gas_limit {
            header.gas_limit - parent_gas_limit
        } else {
            parent_gas_limit - header.gas_limit
        };
        if gas_delta >= parent_gas_limit / 1024 {
            return Err(ValidationError::InvalidGasLimit.into());
        }

        let expected_base_fee_per_gas = self.expected_base_fee_per_gas(header, parent)?;
        if header.base_fee_per_gas != expected_base_fee_per_gas {
            return Err(ValidationError::WrongBaseFee {
                expected: expected_base_fee_per_gas,
                got: header.base_fee_per_gas,
            }
            .into());
        }

        Ok(())
    }

    // See [YP] Section 11.1 "Ommer Validation"
    pub fn is_kin(
        &self,
        branch_header: &BlockHeader,
        mainline_header: &BlockHeader,
        mainline_hash: H256,
        n: usize,
        state: &dyn BlockReader,
        old_ommers: &mut Vec<BlockHeader>,
    ) -> anyhow::Result<bool> {
        if n > 0 && branch_header != mainline_header {
            if let Some(mainline_body) = state.read_body(mainline_header.number, mainline_hash)? {
                old_ommers.extend_from_slice(&mainline_body.ommers);

                let mainline_parent = state.read_parent_header(mainline_header)?;
                let branch_parent = state.read_parent_header(branch_header)?;

                if let Some(mainline_parent) = mainline_parent {
                    if let Some(branch_parent) = branch_parent {
                        if branch_parent == mainline_parent {
                            return Ok(true);
                        }
                    }

                    return self.is_kin(
                        branch_header,
                        &mainline_parent,
                        mainline_header.parent_hash,
                        n - 1,
                        state,
                        old_ommers,
                    );
                }
            }
        }

        Ok(false)
    }

    pub fn get_beneficiary(&self, header: &BlockHeader) -> Address {
        header.beneficiary
    }

    // https://eips.ethereum.org/EIPS/eip-1559
    fn expected_base_fee_per_gas(
        &self,
        header: &BlockHeader,
        parent: &BlockHeader,
    ) -> Result<Option<U256>, DuoError> {
        if let Some(fork_block) = self.eip1559_block {
            if header.number >= fork_block {
                if header.number == fork_block {
                    return Ok(Some(param::INITIAL_BASE_FEE.into()));
                }

                let parent_gas_target = parent.gas_limit / param::ELASTICITY_MULTIPLIER;

                let parent_base_fee_per_gas = parent
                    .base_fee_per_gas
                    .ok_or(DuoError::Validation(ValidationError::MissingBaseFee))?;

                if parent.gas_used == parent_gas_target {
                    return Ok(Some(parent_base_fee_per_gas));
                }

                if parent.gas_used > parent_gas_target {
                    let gas_used_delta = parent.gas_used - parent_gas_target;
                    let base_fee_per_gas_delta = std::cmp::max(
                        U256::ONE,
                        parent_base_fee_per_gas * U256::from(gas_used_delta)
                            / U256::from(parent_gas_target)
                            / U256::from(param::BASE_FEE_MAX_CHANGE_DENOMINATOR),
                    );
                    return Ok(Some(parent_base_fee_per_gas + base_fee_per_gas_delta));
                } else {
                    let gas_used_delta = parent_gas_target - parent.gas_used;
                    let base_fee_per_gas_delta = parent_base_fee_per_gas
                        * U256::from(gas_used_delta)
                        / U256::from(parent_gas_target)
                        / U256::from(param::BASE_FEE_MAX_CHANGE_DENOMINATOR);

                    return Ok(Some(
                        parent_base_fee_per_gas.saturating_sub(base_fee_per_gas_delta),
                    ));
                }
            }
        }

        Ok(None)
    }

    pub fn pre_validate_block(&self, block: &Block) -> Result<(), DuoError> {
        let expected_ommers_hash = Block::ommers_hash(&block.ommers);

        if block.header.ommers_hash != expected_ommers_hash {
            return Err(ValidationError::WrongOmmersHash {
                expected: expected_ommers_hash,
                got: block.header.ommers_hash,
            }
            .into());
        }

        let expected_transactions_root = root_hash(&block.transactions);
        if block.header.transactions_root != expected_transactions_root {
            return Err(ValidationError::WrongTransactionsRoot {
                expected: expected_transactions_root,
                got: block.header.transactions_root,
            }
            .into());
        }

        for txn in &block.transactions {
            pre_validate_transaction(txn, self.chain_id, block.header.base_fee_per_gas)?;
        }

        Ok(())
    }
}

#[derive(Debug, From)]
pub struct BlockSchedule<T: Copy + Default>(pub BTreeMap<BlockNumber, T>);

impl<T> BlockSchedule<T>
where
    T: Copy + Default,
{
    pub fn for_block(&self, block_number: BlockNumber) -> T {
        let mut v = T::default();
        for (&item_since, &item) in &self.0 {
            if item_since <= block_number {
                v = item;
            } else {
                break;
            }
        }
        v
    }
}

pub type BlockRewardSchedule = BlockSchedule<U256>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::res::chainspec::MAINNET;

    #[test]
    fn validate_max_fee_per_gas() {
        let base_fee_per_gas = 1_000_000_000_u64;

        for (max_priority_fee_per_gas, max_fee_per_gas, error, not) in [
            (
                500_000_000_u64,
                700_000_000_u64,
                ValidationError::MaxFeeLessThanBase,
                false,
            ),
            (
                3_000_000_000_u64,
                2_000_000_000_u64,
                ValidationError::MaxPriorityFeeGreaterThanMax,
                false,
            ),
            (
                2_000_000_000_u64,
                2_000_000_000_u64,
                ValidationError::MaxPriorityFeeGreaterThanMax,
                true,
            ),
            (
                1_000_000_000_u64,
                2_000_000_000_u64,
                ValidationError::MaxPriorityFeeGreaterThanMax,
                true,
            ),
        ] {
            let txn = Message::EIP1559 {
                chain_id: ChainId(1),
                nonce: 0,
                max_priority_fee_per_gas: max_priority_fee_per_gas.into(),
                max_fee_per_gas: max_fee_per_gas.into(),
                gas_limit: 0,
                action: TransactionAction::Create,
                value: U256::ZERO,
                input: vec![].into(),
                access_list: vec![],
            };

            let res = pre_validate_transaction(
                &txn,
                MAINNET.params.chain_id,
                Some(base_fee_per_gas.into()),
            );

            if not {
                assert_ne!(res, Err(error));
            } else {
                assert_eq!(res, Err(error));
            }
        }
    }

    #[test]
    fn block_reward() {
        let schedule = BlockSchedule(
            [(0, 500), (10, 200), (20, 0)]
                .into_iter()
                .map(|(b, r)| (BlockNumber(b), r.as_u256()))
                .collect(),
        );

        for (block, expected_reward) in [(0, 500), (5, 500), (10, 200), (15, 200), (20, 0), (25, 0)]
        {
            assert_eq!(schedule.for_block(BlockNumber(block)), expected_reward);
        }
    }
}
