use super::{consensus::*, difficulty::*, intrinsic_gas::*, protocol_param::param};
use crate::{models::*, state::*};
use anyhow::Context;
use async_recursion::*;
use ethereum_types::*;
use evmodin::Revision;
use std::fmt::Display;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum ValidationError {
    // See [YP] Section 4.3.2 "Holistic Validity", Eq (31)
    WrongStateRoot {
        expected: H256,
        got: H256,
    }, // wrong Hr
    WrongOmmersHash {
        expected: H256,
        got: H256,
    }, // wrong Ho
    WrongTransactionsRoot {
        expected: H256,
        got: H256,
    }, // wrong Ht
    WrongReceiptsRoot {
        expected: H256,
        got: H256,
    }, // wrong He
    WrongLogsBloom {
        expected: Bloom,
        got: Bloom,
    }, // wrong Hb

    // See [YP] Section 4.3.4 "Block Header Validity", Eq (50)
    UnknownParent,   // P(H) = ∅ ∨ Hi ≠ P(H)Hi + 1
    WrongDifficulty, // Hd ≠ D(H)
    GasAboveLimit {
        used: u64,
        limit: u64,
    }, // Hg > Hl
    InvalidGasLimit, // |Hl-P(H)Hl|≥P(H)Hl/1024 ∨ Hl<5000
    InvalidTimestamp {
        parent: u64,
        current: u64,
    }, // Hs ≤ P(H)Hs
    ExtraDataTooLong, // ‖Hx‖ > 32
    WrongDaoExtraData, // see EIP-779
    WrongBaseFee {
        expected: Option<U256>,
        got: Option<U256>,
    }, // see EIP-1559
    InvalidSeal,     // Nonce or mix_hash

    // See [YP] Section 6.2 "Execution", Eq (58)
    MissingSender, // S(T) = ∅
    WrongNonce {
        account: Address,
        expected: u64,
        got: u64,
    }, // Tn ≠ σ[S(T)]n
    IntrinsicGas,  // g0 > Tg
    InsufficientFunds {
        account: Address,
        available: U512,
        required: U512,
    }, // v0 > σ[S(T)]b
    BlockGasLimitExceeded {
        available: u64,
        required: u64,
    }, // Tg > BHl - l(BR)u
    MaxFeeLessThanBase, // max_fee_per_gas < base_fee_per_gas (EIP-1559)
    MaxPriorityFeeGreaterThanMax, // max_priority_fee_per_gas > max_fee_per_gas (EIP-1559)

    // See [YP] Section 11.1 "Ommer Validation", Eq (157)
    TooManyOmmers,      // ‖BU‖ > 2
    InvalidOmmerHeader, // ¬V(U)
    NotAnOmmer,         // ¬k(U, P(BH)H, 6)
    DuplicateOmmer,     // not well covered by the YP actually

    // See [YP] Section 11.2 "Transaction Validation", Eq (160)
    WrongBlockGas {
        expected: u64,
        got: u64,
    }, // BHg ≠ l(BR)u

    InvalidSignature, // EIP-2

    WrongChainId, // EIP-155

    UnsupportedTransactionType, // EIP-2718
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ValidationError {}

pub fn pre_validate_transaction(
    txn: &TransactionMessage,
    block_number: impl Into<BlockNumber>,
    config: &ChainConfig,
    base_fee_per_gas: Option<U256>,
) -> Result<(), ValidationError> {
    let rev = config.revision(block_number);

    if let Some(chain_id) = txn.chain_id() {
        if rev < Revision::Spurious || chain_id != config.chain_id {
            return Err(ValidationError::WrongChainId);
        }
    }

    match txn.tx_type() {
        TxType::EIP2930 => {
            if rev < Revision::Berlin {
                return Err(ValidationError::UnsupportedTransactionType);
            }
        }
        TxType::EIP1559 => {
            if rev < Revision::London {
                return Err(ValidationError::UnsupportedTransactionType);
            }
        }
        TxType::Legacy => {}
    }

    if let Some(base_fee_per_gas) = base_fee_per_gas {
        if txn.max_fee_per_gas() < base_fee_per_gas {
            return Err(ValidationError::MaxFeeLessThanBase);
        }
    }

    // https://github.com/ethereum/EIPs/pull/3594
    if txn.max_priority_fee_per_gas() > txn.max_fee_per_gas() {
        return Err(ValidationError::MaxPriorityFeeGreaterThanMax);
    }

    let g0 = intrinsic_gas(txn, rev >= Revision::Homestead, rev >= Revision::Istanbul);
    if u128::from(txn.gas_limit()) < g0 {
        return Err(ValidationError::IntrinsicGas);
    }

    Ok(())
}

async fn get_parent<'storage, S>(
    state: &S,
    header: &BlockHeader,
) -> anyhow::Result<Option<BlockHeader>>
where
    S: State<'storage>,
{
    if let Some(parent_number) = header.number.0.checked_sub(1) {
        return state
            .read_header(parent_number.into(), header.parent_hash)
            .await;
    }

    Ok(None)
}

// https://eips.ethereum.org/EIPS/eip-1559
fn expected_base_fee_per_gas(
    header: &BlockHeader,
    parent: &BlockHeader,
    config: &ChainConfig,
) -> Option<U256> {
    if let Some(fork_block) = config.london_block {
        if header.number >= fork_block {
            if header.number == fork_block {
                return Some(param::INITIAL_BASE_FEE.into());
            }

            let parent_gas_target = parent.gas_limit / param::ELASTICITY_MULTIPLIER;

            let parent_base_fee_per_gas = parent.base_fee_per_gas.unwrap();

            if parent.gas_used == parent_gas_target {
                return Some(parent_base_fee_per_gas);
            }

            if parent.gas_used > parent_gas_target {
                let gas_used_delta = parent.gas_used - parent_gas_target;
                let base_fee_per_gas_delta = std::cmp::max(
                    U256::one(),
                    parent_base_fee_per_gas * U256::from(gas_used_delta)
                        / U256::from(parent_gas_target)
                        / U256::from(param::BASE_FEE_MAX_CHANGE_DENOMINATOR),
                );
                return Some(parent_base_fee_per_gas + base_fee_per_gas_delta);
            } else {
                let gas_used_delta = parent_gas_target - parent.gas_used;
                let base_fee_per_gas_delta = parent_base_fee_per_gas * U256::from(gas_used_delta)
                    / U256::from(parent_gas_target)
                    / U256::from(param::BASE_FEE_MAX_CHANGE_DENOMINATOR);

                return Some(parent_base_fee_per_gas.saturating_sub(base_fee_per_gas_delta));
            }
        }
    }

    None
}

async fn validate_block_header<'storage, C: Consensus, S: State<'storage>>(
    consensus: &C,
    header: &BlockHeader,
    state: &S,
    config: &ChainConfig,
) -> anyhow::Result<()> {
    if header.gas_used > header.gas_limit {
        return Err(ValidationError::GasAboveLimit {
            used: header.gas_used,
            limit: header.gas_limit,
        }
        .into());
    }

    if header.gas_limit < 5000 {
        return Err(ValidationError::InvalidGasLimit.into());
    }

    // https://github.com/ethereum/go-ethereum/blob/v1.9.25/consensus/ethash/consensus.go#L267
    // https://eips.ethereum.org/EIPS/eip-1985
    if header.gas_limit > 0x7fffffffffffffff {
        return Err(ValidationError::InvalidGasLimit.into());
    }

    if header.extra_data.len() > 32 {
        return Err(ValidationError::ExtraDataTooLong.into());
    }

    let parent = get_parent(state, header)
        .await?
        .ok_or(ValidationError::UnknownParent)?;

    if header.timestamp <= parent.timestamp {
        return Err(ValidationError::InvalidTimestamp {
            parent: parent.timestamp,
            current: header.timestamp,
        }
        .into());
    }

    let mut parent_gas_limit = parent.gas_limit;
    if let Some(fork_block) = config.london_block {
        if fork_block == header.number {
            parent_gas_limit = parent.gas_limit * param::ELASTICITY_MULTIPLIER; // EIP-1559
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

    let parent_has_uncles = parent.ommers_hash != EMPTY_LIST_HASH;
    let difficulty = canonical_difficulty(
        header.number,
        header.timestamp,
        parent.difficulty,
        parent.timestamp,
        parent_has_uncles,
        config,
    );
    if difficulty != header.difficulty {
        return Err(ValidationError::WrongDifficulty.into());
    }

    let expected_base_fee_per_gas = expected_base_fee_per_gas(header, &parent, config);
    if header.base_fee_per_gas != expected_base_fee_per_gas {
        return Err(ValidationError::WrongBaseFee {
            expected: expected_base_fee_per_gas,
            got: header.base_fee_per_gas,
        }
        .into());
    }

    consensus.verify_header(header).await?;

    Ok(())
}

// See [YP] Section 11.1 "Ommer Validation"
#[async_recursion]
async fn is_kin<'storage, S: State<'storage>>(
    branch_header: &BlockHeader,
    mainline_header: &BlockHeader,
    mainline_hash: H256,
    n: usize,
    state: &S,
    old_ommers: &mut Vec<BlockHeader>,
) -> anyhow::Result<bool> {
    if n > 0 && branch_header != mainline_header {
        if let Some(mainline_body) = state
            .read_body(mainline_header.number, mainline_hash)
            .await?
        {
            old_ommers.extend_from_slice(&mainline_body.ommers);

            let mainline_parent = get_parent(state, mainline_header).await?;
            let branch_parent = get_parent(state, branch_header).await?;

            if let Some(mainline_parent) = mainline_parent {
                if let Some(branch_parent) = branch_parent {
                    if branch_parent == mainline_parent {
                        return Ok(true);
                    }
                }

                return is_kin(
                    branch_header,
                    &mainline_parent,
                    mainline_header.parent_hash,
                    n - 1,
                    state,
                    old_ommers,
                )
                .await;
            }
        }
    }

    Ok(false)
}

pub async fn pre_validate_block<'storage, C: Consensus, S: State<'storage>>(
    consensus: &C,
    block: &Block,
    state: &S,
    config: &ChainConfig,
) -> anyhow::Result<()> {
    validate_block_header(consensus, &block.header, state, config).await?;

    let expected_ommers_hash = Block::ommers_hash(&block.ommers);
    if block.header.ommers_hash != expected_ommers_hash {
        return Err(ValidationError::WrongOmmersHash {
            expected: expected_ommers_hash,
            got: block.header.ommers_hash,
        }
        .into());
    }

    let expected_transactions_root = Block::transactions_root(&block.transactions);
    if block.header.transactions_root != expected_transactions_root {
        return Err(ValidationError::WrongTransactionsRoot {
            expected: expected_transactions_root,
            got: block.header.transactions_root,
        }
        .into());
    }

    if block.ommers.len() > 2 {
        return Err(ValidationError::TooManyOmmers.into());
    }

    if block.ommers.len() == 2 && block.ommers[0] == block.ommers[1] {
        return Err(ValidationError::DuplicateOmmer.into());
    }

    let parent = get_parent(state, &block.header).await?.unwrap();

    for ommer in &block.ommers {
        validate_block_header(consensus, ommer, state, config)
            .await
            .context(ValidationError::InvalidOmmerHeader)?;
        let mut old_ommers = vec![];
        if !is_kin(
            ommer,
            &parent,
            block.header.parent_hash,
            6,
            state,
            &mut old_ommers,
        )
        .await?
        {
            return Err(ValidationError::NotAnOmmer.into());
        }
        for oo in old_ommers {
            if oo == *ommer {
                return Err(ValidationError::DuplicateOmmer.into());
            }
        }
    }

    for txn in &block.transactions {
        pre_validate_transaction(
            txn,
            block.header.number,
            config,
            block.header.base_fee_per_gas,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::chain::config::MAINNET_CONFIG;

    use super::*;

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
            let txn = TransactionMessage::EIP1559 {
                chain_id: 1,
                nonce: 0,
                max_priority_fee_per_gas: max_priority_fee_per_gas.into(),
                max_fee_per_gas: max_fee_per_gas.into(),
                gas_limit: 0,
                action: TransactionAction::Create,
                value: U256::zero(),
                input: vec![].into(),
                access_list: vec![],
            };

            let res = pre_validate_transaction(
                &txn,
                13_500_001,
                &*MAINNET_CONFIG,
                Some(base_fee_per_gas.into()),
            );

            if not {
                assert_ne!(res, Err(error));
            } else {
                assert_eq!(res, Err(error));
            }
        }
    }
}
