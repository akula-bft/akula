use super::header::BlockHeader;
use crate::{
    consensus::difficulty::{canonical_difficulty, BlockDifficultyBombData},
    models::{switch_is_active, BlockNumber, ChainSpec, SealVerificationParams, EMPTY_LIST_HASH},
};

pub fn verify_link_by_parent_hash(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let given_parent_hash = child.parent_hash();
    let expected_parent_hash = parent.hash();
    given_parent_hash == expected_parent_hash
}

pub fn verify_link_block_nums(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let given_block_num = child.number().0;
    let expected_block_num = parent.number().0 + 1;
    given_block_num == expected_block_num
}

pub fn verify_link_timestamps(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let parent_timestamp = parent.timestamp();
    let child_timestamp = child.timestamp();
    parent_timestamp < child_timestamp
}

pub fn verify_link_difficulties(
    child: &BlockHeader,
    parent: &BlockHeader,
    chain_spec: &ChainSpec,
) -> bool {
    let (&byzantium_formula, &homestead_formula, difficulty_bomb) =
        match &chain_spec.consensus.seal_verification {
            SealVerificationParams::Ethash {
                byzantium_formula,
                homestead_formula,
                difficulty_bomb,
                ..
            } => (byzantium_formula, homestead_formula, difficulty_bomb),
            _ => {
                panic!("unsupported consensus engine");
            }
        };

    let given_child_difficulty = child.difficulty();
    let expected_child_difficulty = canonical_difficulty(
        child.number(),
        child.timestamp(),
        parent.difficulty(),
        parent.timestamp(),
        parent.ommers_hash() != EMPTY_LIST_HASH,
        switch_is_active(byzantium_formula, child.number()),
        switch_is_active(homestead_formula, child.number()),
        difficulty_bomb
            .as_ref()
            .map(|bomb| BlockDifficultyBombData {
                delay_to: bomb.get_delay_to(child.number()),
            }),
    );
    given_child_difficulty == expected_child_difficulty
}

pub fn verify_link_pow(_child: &BlockHeader, _parent: &BlockHeader) -> bool {
    // TODO: verify_link_pow
    true
}

fn enumerate_sequential_pairs(
    headers: &[BlockHeader],
) -> impl Iterator<Item = (&BlockHeader, &BlockHeader)> {
    let prev_it = headers.iter();
    let next_it = headers.iter().skip(1);
    prev_it.zip(next_it)
}

/// Verify that all blocks in the slice are linked by the parent_hash field.
pub fn verify_slice_is_linked_by_parent_hash(headers: &[BlockHeader]) -> bool {
    enumerate_sequential_pairs(headers)
        .all(|(parent, child)| verify_link_by_parent_hash(child, parent))
}

/// Verify that block numbers start from the expected
/// slice.start_block_num and increase sequentially.
pub fn verify_slice_block_nums(headers: &[BlockHeader], start_block_num: BlockNumber) -> bool {
    if headers.is_empty() {
        return true;
    }

    for (parent, child) in enumerate_sequential_pairs(headers) {
        if !verify_link_block_nums(child, parent) {
            return false;
        }
    }

    // verify the first block number
    let first = &headers[0];
    let first_block_num = first.number();
    first_block_num == start_block_num
}

/// Verify that timestamps are in the past and increase monotonically.
pub fn verify_slice_timestamps(headers: &[BlockHeader], max_timestamp: u64) -> bool {
    if headers.is_empty() {
        return true;
    }

    for (parent, child) in enumerate_sequential_pairs(headers) {
        if !verify_link_timestamps(child, parent) {
            return false;
        }
    }

    let last = headers.last().unwrap();
    let last_timestamp = last.timestamp();
    last_timestamp < max_timestamp
}

/// Verify that difficulty field is calculated properly.
pub fn verify_slice_difficulties(headers: &[BlockHeader], chain_spec: &ChainSpec) -> bool {
    enumerate_sequential_pairs(headers)
        .all(|(parent, child)| verify_link_difficulties(child, parent, chain_spec))
}

/// Verify the headers proof-of-work.
pub fn verify_slice_pow(_headers: &[BlockHeader]) -> bool {
    // TODO: verify_slice_pow
    true
}
