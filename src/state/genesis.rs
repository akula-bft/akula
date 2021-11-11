use crate::{
    kv::tables::{self, CumulativeData, PlainStateFusedValue},
    models::{ChainConfig, *},
    util::*,
    InMemoryState, MutableCursor, MutableTransaction,
};
use bytes::Bytes;
use ethereum_types::*;
use serde::*;
use std::collections::HashMap;

pub trait GenesisState {
    fn initial_state(&self) -> InMemoryState;
    fn header(&self, initial_state: &InMemoryState) -> BlockHeader;
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AllocAccountData {
    pub balance: U256,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenesisData {
    pub alloc: HashMap<Address, AllocAccountData>,
    pub coinbase: Address,
    pub config: ChainConfig,
    pub difficulty: U256,
    #[serde(deserialize_with = "deserialize_hexstr_as_bytes")]
    pub extra_data: Bytes,
    #[serde(deserialize_with = "deserialize_hexstr_as_u64")]
    pub gas_limit: u64,
    pub mix_hash: H256,
    pub nonce: H64,
    pub parent_hash: H256,
    #[serde(deserialize_with = "deserialize_hexstr_as_u64")]
    pub timestamp: u64,
}

impl GenesisState for GenesisData {
    fn initial_state(&self) -> InMemoryState {
        let mut state_buffer = InMemoryState::new();
        // Allocate accounts
        for (address, account) in &self.alloc {
            let current_account = Account {
                balance: account.balance,
                ..Default::default()
            };
            state_buffer.update_account_sync(*address, None, Some(current_account));
        }
        state_buffer
    }

    fn header(&self, initial_state: &InMemoryState) -> BlockHeader {
        let genesis = self;
        let state_root = initial_state.state_root_hash();

        BlockHeader {
            parent_hash: H256::zero(),
            beneficiary: genesis.coinbase,
            state_root,
            logs_bloom: Bloom::zero(),
            difficulty: genesis.difficulty,
            number: BlockNumber(0),
            gas_limit: genesis.gas_limit,
            gas_used: 0,
            timestamp: genesis.timestamp,
            extra_data: genesis.extra_data.clone(),
            mix_hash: genesis.mix_hash,
            nonce: genesis.nonce,
            base_fee_per_gas: None,

            receipts_root: EMPTY_ROOT,
            ommers_hash: EMPTY_LIST_HASH,
            transactions_root: EMPTY_ROOT,
        }
    }
}

pub async fn initialize_genesis<'db, Tx>(txn: &Tx, genesis: GenesisData) -> anyhow::Result<bool>
where
    Tx: MutableTransaction<'db>,
{
    if txn
        .get(&tables::CanonicalHeader, BlockNumber(0))
        .await?
        .is_some()
    {
        return Ok(false);
    }

    let state_buffer = genesis.initial_state();

    // Write allocations to db - no changes only accounts
    let mut state_table = txn.mutable_cursor(&tables::PlainState).await?;
    for (address, account) in state_buffer.accounts() {
        // Store account plain state
        state_table
            .upsert(PlainStateFusedValue::Account {
                address,
                account: account.encode_for_storage(false),
            })
            .await?;
    }

    let header = genesis.header(&state_buffer);
    let block_hash = header.hash();

    txn.set(&tables::Header, ((0.into(), block_hash), header.clone()))
        .await?;
    txn.set(&tables::CanonicalHeader, (0.into(), block_hash))
        .await?;
    txn.set(&tables::HeaderNumber, (block_hash, 0.into()))
        .await?;
    txn.set(
        &tables::HeadersTotalDifficulty,
        ((0.into(), block_hash), header.difficulty),
    )
    .await?;

    txn.set(
        &tables::BlockBody,
        (
            (0.into(), block_hash),
            BodyForStorage {
                base_tx_id: 0.into(),
                tx_amount: 0,
                uncles: vec![],
            },
        ),
    )
    .await?;

    txn.set(
        &tables::CumulativeIndex,
        (0.into(), CumulativeData { gas: 0, tx_num: 0 }),
    )
    .await?;

    txn.set(&tables::LastHeader, (Default::default(), block_hash))
        .await?;

    txn.set(&tables::Config, (block_hash, genesis.config))
        .await?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::traits::{MutableKV, Transaction},
        new_mem_database,
    };
    use hex_literal::hex;

    #[tokio::test]
    async fn init_mainnet_genesis() {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        assert!(
            initialize_genesis(&tx, crate::res::genesis::MAINNET.clone())
                .await
                .unwrap()
        );

        let genesis_hash = tx
            .get(&tables::CanonicalHeader, 0.into())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            genesis_hash,
            hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3").into()
        );
    }

    #[test]
    fn mainnet_genesis_hash() {
        let genesis = &crate::res::genesis::MAINNET;
        let genesis_hash = genesis.header(&genesis.initial_state()).hash();
        assert_eq!(
            genesis_hash,
            hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3").into()
        );
    }

    // TODO: fix
    //#[test]
    fn ropsten_genesis_hash() {
        let genesis = &crate::res::genesis::ROPSTEN;
        let genesis_hash = genesis.header(&genesis.initial_state()).hash();
        assert_eq!(
            genesis_hash,
            hex!("41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d").into()
        );
    }
}
