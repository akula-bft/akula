use crate::{
    mining::state::MiningConfig,
    models::{BlockHeader, BlockNumber},
};
use anyhow::bail;
use bytes::Bytes;
use ethereum_types::Address;
use ethnum::U256;
use primitive_types::H256;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug)]
pub struct BlockProposal {
    parent_hash: H256,
    number: BlockNumber,
    beneficiary: Address,
    difficulty: U256,
    extra_data: Bytes,
    timestamp: u64,
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub fn get_difficulty(parent_header: &BlockHeader, _config: &MiningConfig) -> anyhow::Result<U256> {
    Ok(parent_header.difficulty + 1) // TODO
}

pub fn create_proposal(
    parent_header: &BlockHeader,
    config: &MiningConfig,
) -> anyhow::Result<BlockProposal> {
    let timestamp = now();
    if timestamp <= parent_header.timestamp {
        bail!("Current system time is earlier than existing block timestamp.");
    }

    let difficulty = get_difficulty(parent_header, config)?;

    Ok(BlockProposal {
        parent_hash: parent_header.hash(),
        number: parent_header.number + 1,
        beneficiary: config.get_ether_base(),
        difficulty,
        extra_data: config.get_extra_data(),
        timestamp,
    })
}
