mod base;
mod blockchain;
mod ethash;

pub use self::{blockchain::*, ethash::*};
use crate::{models::*, State};
use anyhow::bail;
use async_trait::async_trait;
use evmodin::Revision;
use std::fmt::{Debug, Display};

#[derive(Debug)]
pub enum FinalizationChange {
    Reward { address: Address, amount: U256 },
}

#[async_trait]
pub trait Consensus: Debug + Send + Sync + 'static {
    /// Performs validation of block header & body that can be done prior to sender recovery and execution.
    /// See [YP] Sections 4.3.2 "Holistic Validity", 4.3.4 "Block Header Validity", and 11.1 "Ommer Validation".
    ///
    /// NOTE: Shouldn't be used for genesis block.
    async fn pre_validate_block(&self, block: &Block, state: &mut dyn State) -> anyhow::Result<()>;

    /// See [YP] Section 4.3.4 "Block Header Validity".
    ///
    /// NOTE: Shouldn't be used for genesis block.
    async fn validate_block_header(
        &self,
        header: &BlockHeader,
        state: &mut dyn State,
        with_future_timestamp_check: bool,
    ) -> anyhow::Result<()>;

    /// Validates the seal of the header
    async fn validate_seal(&self, header: &BlockHeader) -> anyhow::Result<()>;

    /// Finalizes block execution by applying changes in the state of accounts or of the consensus itself
    ///
    /// NOTE: For Ethash See [YP] Section 11.3 "Reward Application".
    async fn finalize(
        &self,
        block: &PartialHeader,
        ommers: &[BlockHeader],
        revision: Revision,
    ) -> anyhow::Result<Vec<FinalizationChange>>;

    /// See [YP] Section 11.3 "Reward Application".
    async fn get_beneficiary(&self, header: &BlockHeader) -> anyhow::Result<Address>;
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum ValidationError {
    FutureBlock {
        now: u64,
        got: u64,
    }, // Block has a timestamp in the future

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
    SenderNoEOA {
        sender: Address,
    }, // EIP-3607: σ[S(T)]c ≠ KEC( () )
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
        transactions: Vec<(usize, u64)>,
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
    txn: &Message,
    canonical_chain_id: ChainId,
    base_fee_per_gas: Option<U256>,
) -> Result<(), ValidationError> {
    if let Some(chain_id) = txn.chain_id() {
        if chain_id != canonical_chain_id {
            return Err(ValidationError::WrongChainId);
        }
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

    Ok(())
}

pub fn engine_factory(chain_config: ChainSpec) -> anyhow::Result<Box<dyn Consensus>> {
    Ok(match chain_config.consensus.seal_verification {
        SealVerificationParams::Ethash {
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_formula,
            difficulty_bomb,
            skip_pow_verification,
        } => Box::new(Ethash::new(
            chain_config.params.chain_id,
            chain_config.consensus.eip1559_block,
            duration_limit,
            block_reward,
            homestead_formula,
            byzantium_formula,
            difficulty_bomb,
            skip_pow_verification,
        )),
        _ => bail!("unsupported consensus engine"),
    })
}
