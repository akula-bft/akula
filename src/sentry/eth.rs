use super::devp2p::*;
use crate::models::*;
use anyhow::anyhow;
use arrayvec::ArrayString;
use enum_primitive_derive::*;
use ethereum_forkid::{ForkFilter, ForkId};
use fastrlp::*;
use std::{collections::BTreeSet, convert::TryFrom};

pub fn capability_name() -> CapabilityName {
    CapabilityName(ArrayString::from("eth").unwrap())
}

#[derive(Clone, Debug, RlpEncodable, RlpDecodable, RlpMaxEncodedLen)]
pub struct StatusMessage {
    pub protocol_version: usize,
    pub network_id: u64,
    pub total_difficulty: U256,
    pub best_hash: H256,
    pub genesis_hash: H256,
    pub fork_id: ForkId,
}

#[derive(Clone, Debug)]
pub struct Forks {
    pub genesis: H256,
    pub forks: BTreeSet<u64>,
}

#[derive(Clone, Debug)]
pub struct StatusData {
    pub network_id: u64,
    pub total_difficulty: U256,
    pub best_hash: H256,
    pub fork_data: Forks,
}

#[derive(Clone, Debug)]
pub struct FullStatusData {
    pub status: StatusData,
    pub fork_filter: ForkFilter,
}

impl TryFrom<ethereum_interfaces::sentry::StatusData> for FullStatusData {
    type Error = anyhow::Error;

    fn try_from(value: ethereum_interfaces::sentry::StatusData) -> Result<Self, Self::Error> {
        let ethereum_interfaces::sentry::StatusData {
            network_id,
            total_difficulty,
            best_hash,
            fork_data,
            max_block,
        } = value;

        let fork_data = fork_data.ok_or_else(|| anyhow!("no fork data"))?;
        let genesis = fork_data
            .genesis
            .ok_or_else(|| anyhow!("no genesis"))?
            .into();

        let fork_filter = ForkFilter::new(max_block, genesis, fork_data.forks.clone());
        let status = StatusData {
            network_id,
            total_difficulty: total_difficulty
                .ok_or_else(|| anyhow!("no total difficulty"))?
                .into(),
            best_hash: best_hash.ok_or_else(|| anyhow!("no best hash"))?.into(),
            fork_data: Forks {
                genesis,
                forks: fork_data.forks.into_iter().collect(),
            },
        };

        Ok(Self {
            status,
            fork_filter,
        })
    }
}

#[derive(Clone, Copy, Debug, Primitive)]
pub enum EthMessageId {
    Status = 0,
    NewBlockHashes = 1,
    Transactions = 2,
    GetBlockHeaders = 3,
    BlockHeaders = 4,
    GetBlockBodies = 5,
    BlockBodies = 6,
    NewBlock = 7,
    NewPooledTransactionHashes = 8,
    GetPooledTransactions = 9,
    PooledTransactions = 10,
    GetNodeData = 13,
    NodeData = 14,
    GetReceipts = 15,
    Receipts = 16,
}

#[derive(Clone, Copy, Debug, Primitive)]
pub enum EthProtocolVersion {
    Eth65 = 65,
    Eth66 = 66,
}
