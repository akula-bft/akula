use super::{
    super::headers::header::BlockHeader, preverified_hashes_config::PreverifiedHashesConfig,
};
use crate::{
    consensus::difficulty::{canonical_difficulty, BlockDifficultyBombData},
    models::{switch_is_active, BlockNumber, ChainSpec, SealVerificationParams, EMPTY_LIST_HASH},
};
use std::fmt::Debug;

pub trait HeaderSliceVerifier: Send + Sync + Debug {
    fn verify_link(
        &self,
        child: &BlockHeader,
        parent: &BlockHeader,
        chain_spec: &ChainSpec,
    ) -> bool;

    fn verify_slice(
        &self,
        headers: &[BlockHeader],
        start_block_num: BlockNumber,
        max_timestamp: u64,
        chain_spec: &ChainSpec,
    ) -> bool;

    fn preverified_hashes_config(
        &self,
        chain_name: &str,
    ) -> anyhow::Result<PreverifiedHashesConfig>;
}

pub fn make_ethash_verifier() -> Box<dyn HeaderSliceVerifier> {
    Box::new(EthashHeaderSliceVerifier {})
}

#[derive(Debug)]
struct EthashHeaderSliceVerifier;

impl HeaderSliceVerifier for EthashHeaderSliceVerifier {
    fn verify_link(
        &self,
        child: &BlockHeader,
        parent: &BlockHeader,
        chain_spec: &ChainSpec,
    ) -> bool {
        verify_link_by_parent_hash(child, parent)
            && verify_link_block_nums(child, parent)
            && verify_link_timestamps(child, parent)
            && verify_link_difficulties(child, parent, chain_spec)
            && verify_link_pow(child, parent)
    }

    fn verify_slice(
        &self,
        headers: &[BlockHeader],
        start_block_num: BlockNumber,
        max_timestamp: u64,
        chain_spec: &ChainSpec,
    ) -> bool {
        verify_slice_is_linked_by_parent_hash(headers)
            && verify_slice_block_nums(headers, start_block_num)
            && verify_slice_timestamps(headers, max_timestamp)
            && verify_slice_difficulties(headers, chain_spec)
            && verify_slice_pow(headers)
    }

    fn preverified_hashes_config(
        &self,
        chain_name: &str,
    ) -> anyhow::Result<PreverifiedHashesConfig> {
        PreverifiedHashesConfig::new(chain_name)
    }
}

fn verify_link_by_parent_hash(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let given_parent_hash = child.parent_hash();
    let expected_parent_hash = parent.hash();
    given_parent_hash == expected_parent_hash
}

fn verify_link_block_nums(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let given_block_num = child.number().0;
    let expected_block_num = parent.number().0 + 1;
    given_block_num == expected_block_num
}

fn verify_link_timestamps(child: &BlockHeader, parent: &BlockHeader) -> bool {
    let parent_timestamp = parent.timestamp();
    let child_timestamp = child.timestamp();
    parent_timestamp < child_timestamp
}

fn verify_link_difficulties(
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

fn verify_link_pow(_child: &BlockHeader, _parent: &BlockHeader) -> bool {
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
fn verify_slice_block_nums(headers: &[BlockHeader], start_block_num: BlockNumber) -> bool {
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
fn verify_slice_timestamps(headers: &[BlockHeader], max_timestamp: u64) -> bool {
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
fn verify_slice_difficulties(headers: &[BlockHeader], chain_spec: &ChainSpec) -> bool {
    enumerate_sequential_pairs(headers)
        .all(|(parent, child)| verify_link_difficulties(child, parent, chain_spec))
}

/// Verify the headers proof-of-work.
fn verify_slice_pow(_headers: &[BlockHeader]) -> bool {
    // TODO: verify_slice_pow
    true
}
