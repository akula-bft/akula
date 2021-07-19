use ethereum_types::*;
use std::fmt::Display;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum ValidationError {
    // See [YP] Section 4.3.2 "Holistic Validity", Eq (31)
    WrongStateRoot { expected: H256, got: H256 }, // wrong Hr
    WrongOmmersHash { expected: H256, got: H256 }, // wrong Ho
    WrongTransactionsRoot { expected: H256, got: H256 }, // wrong Ht
    WrongReceiptsRoot { expected: H256, got: H256 }, // wrong He
    WrongLogsBloom { expected: Bloom, got: Bloom }, // wrong Hb

    // See [YP] Section 4.3.4 "Block Header Validity", Eq (50)
    UnknownParent,     // P(H) = ∅ ∨ Hi ≠ P(H)Hi + 1
    WrongDifficulty,   // Hd ≠ D(H)
    GasAboveLimit,     // Hg > Hl
    InvalidGasLimit,   // |Hl-P(H)Hl|≥P(H)Hl/1024 ∨ Hl<5000
    InvalidTimestamp,  // Hs ≤ P(H)Hs
    ExtraDataTooLong,  // ‖Hx‖ > 32
    WrongDaoExtraData, // see EIP-779
    WrongBaseFee,      // see EIP-1559
    InvalidSeal,       // Nonce or mix_hash

    // See [YP] Section 6.2 "Execution", Eq (58)
    MissingSender,                                           // S(T) = ∅
    WrongNonce { expected: u64, got: u64 },                  // Tn ≠ σ[S(T)]n
    IntrinsicGas,                                            // g0 > Tg
    InsufficientFunds { available: U512, required: U512 },   // v0 > σ[S(T)]b
    BlockGasLimitExceeded { available: u64, required: u64 }, // Tg > BHl - l(BR)u
    MaxFeeLessThanBase,           // max_fee_per_gas < base_fee_per_gas (EIP-1559)
    MaxPriorityFeeGreaterThanMax, // max_priority_fee_per_gas > max_fee_per_gas (EIP-1559)

    // See [YP] Section 11.1 "Ommer Validation", Eq (157)
    TooManyOmmers,      // ‖BU‖ > 2
    InvalidOmmerHeader, // ¬V(U)
    NotAnOmmer,         // ¬k(U, P(BH)H, 6)
    DuplicateOmmer,     // not well covered by the YP actually

    // See [YP] Section 11.2 "Transaction Validation", Eq (160)
    WrongBlockGas { expected: u64, got: u64 }, // BHg ≠ l(BR)u

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
