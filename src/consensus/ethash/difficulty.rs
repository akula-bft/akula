use crate::models::*;
use std::cmp::max;

const MIN_DIFFICULTY: u64 = 131_072;

pub struct BlockDifficultyBombData {
    pub delay_to: BlockNumber,
}

#[allow(clippy::too_many_arguments)]
pub fn canonical_difficulty(
    block_number: impl Into<BlockNumber>,
    block_timestamp: u64,
    parent_difficulty: U256,
    parent_timestamp: u64,
    parent_has_uncles: bool,
    byzantium_formula: bool,
    homestead_formula: bool,
    difficulty_bomb: Option<BlockDifficultyBombData>,
) -> U256 {
    let block_number = block_number.into();

    let mut difficulty = parent_difficulty;

    let x = parent_difficulty >> 11; // parent_difficulty / 2048;

    if byzantium_formula {
        // Byzantium
        difficulty -= x * 99.as_u256();

        // https://eips.ethereum.org/EIPS/eip-100
        let y = if parent_has_uncles { 2 } else { 1 };
        let z = (block_timestamp - parent_timestamp) / 9;
        if 99 + y > z {
            difficulty += U256::from(99 + y - z) * x;
        }
    } else if homestead_formula {
        // Homestead
        difficulty -= x * 99.as_u256();

        let z = (block_timestamp - parent_timestamp) / 10;
        if 100 > z {
            difficulty += U256::from(100 - z) * x;
        }
    } else {
        // Frontier
        if block_timestamp - parent_timestamp < 13 {
            difficulty += x;
        } else {
            difficulty -= x;
        }
    }

    if let Some(bomb_config) = difficulty_bomb {
        // https://eips.ethereum.org/EIPS/eip-649
        let n = block_number.saturating_sub(bomb_config.delay_to.0) / 100_000;
        if n >= 2 {
            difficulty += U256::ONE << (n - 2);
        }
    }

    max(difficulty, MIN_DIFFICULTY.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::res::chainspec::MAINNET;

    #[test]
    fn difficulty_test() {
        for (
            block_number,
            block_timestamp,
            parent_difficulty,
            parent_timestamp,
            parent_has_uncles,
            expected_difficulty,
        ) in [
            (
                0x33e140,
                0x04bdbdaf,
                0x7268db7b46b0b154_u64.into(),
                0x04bdbdaf,
                false,
                0x72772897b619876a_u64.as_u256(),
            ),
            (
                13636066,
                1637194138,
                11_578_490_198_380_085_u128.into(),
                1637194129,
                false,
                11_578_627_637_333_557_u128.as_u256(),
            ),
        ] {
            let block_number = block_number.into();
            let SealVerificationParams::Ethash { homestead_formula, byzantium_formula, difficulty_bomb, .. } = MAINNET.clone().consensus.seal_verification else {
                unreachable!()
            };

            let difficulty = canonical_difficulty(
                block_number,
                block_timestamp,
                parent_difficulty,
                parent_timestamp,
                parent_has_uncles,
                switch_is_active(byzantium_formula, block_number),
                switch_is_active(homestead_formula, block_number),
                difficulty_bomb.map(|b| BlockDifficultyBombData {
                    delay_to: b.get_delay_to(block_number),
                }),
            );
            assert_eq!(difficulty, expected_difficulty);
        }
    }
}
