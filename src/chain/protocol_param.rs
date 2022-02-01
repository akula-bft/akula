// Gas & refund fee schedule—see Appendix G of the Yellow Paper
// https://ethereum.github.io/yellowpaper/paper.pdf
pub mod fee {

    pub const G_SLOAD_TANGERINE_WHISTLE: u64 = 200;
    pub const G_SLOAD_ISTANBUL: u64 = 800;

    pub const WARM_STORAGE_READ_COST: u64 = 100;
    pub const COLD_SLOAD_COST: u64 = 2100;
    pub const ACCESS_LIST_STORAGE_KEY_COST: u64 = 1900;
    pub const ACCESS_LIST_ADDRESS_COST: u64 = 2400;

    pub const G_SSET: u64 = 20_000;
    pub const G_SRESET: u64 = 5_000;

    pub const R_SCLEAR: u64 = 15_000;
    pub const R_SELF_DESTRUCT: u64 = 24_000;

    pub const G_CODE_DEPOSIT: u64 = 200;

    pub const G_TX_CREATE: u64 = 32_000;
    pub const G_TX_DATA_ZERO: u64 = 4;
    pub const G_TX_DATA_NON_ZERO_FRONTIER: u64 = 68;
    pub const G_TX_DATA_NON_ZERO_ISTANBUL: u64 = 16;
    pub const G_TRANSACTION: u64 = 21_000;
} // namespace fee

pub mod param {
    use crate::models::*;

    // https://eips.ethereum.org/EIPS/eip-170
    pub const MAX_CODE_SIZE: usize = 0x6000;

    pub const BLOCK_REWARD_FRONTIER: u128 = 5 * ETHER;
    pub const BLOCK_REWARD_BYZANTIUM: u128 = 3 * ETHER;
    pub const BLOCK_REWARD_CONSTANTINOPLE: u128 = 2 * ETHER;

    pub const G_QUAD_DIVISOR_BYZANTIUM: u64 = 20; // EIP-198
    pub const G_QUAD_DIVISOR_BERLIN: u64 = 3; // EIP-2565

    // https://eips.ethereum.org/EIPS/eip-3529
    pub const MAX_REFUND_QUOTIENT_FRONTIER: u64 = 2;
    pub const MAX_REFUND_QUOTIENT_LONDON: u64 = 5;

    // https://eips.ethereum.org/EIPS/eip-1559
    pub const INITIAL_BASE_FEE: u64 = 1_000_000_000;
    pub const BASE_FEE_MAX_CHANGE_DENOMINATOR: u64 = 8;
    pub const ELASTICITY_MULTIPLIER: u64 = 2;
}
