use crate::models::*;
use ethereum_types::*;
use evmodin::Revision;

const MIN_DIFFICULTY: u64 = 131_072;

pub fn difficulty_bomb(mut difficulty: U256, block_number: impl Into<BlockNumber>) -> U256 {
    let n = block_number.into().0 / 100_000;
    if n >= 2 {
        difficulty += U256::one() << (n - 2);
    }

    if difficulty < U256::from(MIN_DIFFICULTY) {
        difficulty = U256::from(MIN_DIFFICULTY);
    }
    difficulty
}

pub fn canonical_difficulty_frontier(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
) -> U256 {
    let mut difficulty = parent_difficulty;

    let x = parent_difficulty >> 11; // parent_difficulty / 2048;

    if block_timestamp - parent_timestamp < 13 {
        difficulty += x;
    } else {
        difficulty -= x;
    }

    difficulty_bomb(difficulty, block_number)
}

fn canonical_difficulty_homestead(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
) -> U256 {
    let mut difficulty = parent_difficulty;

    let x = parent_difficulty >> 11; // parent_difficulty / 2048;
    difficulty -= x * 99;

    let z = (block_timestamp - parent_timestamp) / 10;
    if 100 > z {
        difficulty += U256::from(100 - z) * x;
    }

    difficulty_bomb(difficulty, block_number)
}

fn canonical_difficulty_byzantium(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
    parent_has_uncles: bool,
    bomb_delay: impl Into<BlockNumber>,
) -> U256 {
    let mut difficulty = parent_difficulty;

    let x = parent_difficulty >> 11; // parent_difficulty / 2048;
    difficulty -= x * 99;

    // https://eips.ethereum.org/EIPS/eip-100
    let y = if parent_has_uncles { 2 } else { 1 };
    let z = (block_timestamp - parent_timestamp) / 9;
    if 99 + y > z {
        difficulty += U256::from(99 + y - z) * x;
    }

    let bomb_delay = bomb_delay.into();
    let mut block_number = block_number.into();

    // https://eips.ethereum.org/EIPS/eip-649
    if block_number > bomb_delay {
        block_number.0 -= bomb_delay.0;
    } else {
        block_number = 0.into();
    }
    difficulty_bomb(difficulty, block_number)
}

pub fn canonical_difficulty(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
    parent_has_uncles: bool,
    config: &ChainSpec,
) -> U256 {
    let block_number = block_number.into();

    let exec_spec = config.collect_block_spec(block_number);
    let rev = exec_spec.revision;

    if rev >= Revision::Byzantium {
        let mut bomb_delay = BlockNumber(0);
        if let SealVerificationParams::Ethash {
            difficulty_bomb: Some(difficulty_bomb),
            ..
        } = &config.consensus.seal_verification
        {
            for (fork_block_num, delay) in difficulty_bomb.delays.iter().rev() {
                if block_number.0 >= fork_block_num.0 {
                    bomb_delay = *delay;
                    break;
                }
            }
        }

        canonical_difficulty_byzantium(
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
            parent_has_uncles,
            bomb_delay,
        )
    } else if rev >= Revision::Homestead {
        canonical_difficulty_homestead(
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
        )
    } else {
        canonical_difficulty_frontier(
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn difficulty_test_34() {
        let block_number = 0x33e140;
        let block_timestamp = 0x04bdbdaf;
        let parent_difficulty = U256::from(0x7268db7b46b0b154_u64);
        let parent_timestamp = 0x04bdbdaf;
        let parent_has_uncles = false;

        let difficulty = canonical_difficulty(
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
            parent_has_uncles,
            &crate::res::chainspec::MAINNET,
        );
        assert_eq!(difficulty, U256::from(0x72772897b619876a_u64));
    }
}
