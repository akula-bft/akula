use super::*;
use crate::{
    kv::tables::{self, PlainStateFusedValue},
    models::{ChainConfig, *},
    util::*,
    InMemoryState, MutableCursor, MutableTransaction,
};
use bytes::Bytes;
use ethereum_types::*;
use serde::*;
use std::collections::HashMap;

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

    let mut state_buffer = InMemoryState::new();
    // Allocate accounts
    for (address, account) in genesis.alloc {
        let balance = account.balance;

        state_buffer
            .update_account(
                address,
                None,
                Some(Account {
                    balance,
                    ..Default::default()
                }),
            )
            .await?;
    }

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

    let state_root = state_buffer.state_root_hash();

    let header = BlockHeader {
        parent_hash: H256::zero(),
        beneficiary: genesis.coinbase,
        state_root,
        logs_bloom: Bloom::zero(),
        difficulty: genesis.difficulty,
        number: BlockNumber(0),
        gas_limit: genesis.gas_limit,
        gas_used: 0,
        timestamp: genesis.timestamp,
        extra_data: genesis.extra_data,
        mix_hash: genesis.mix_hash,
        nonce: genesis.nonce,
        base_fee_per_gas: None,

        receipts_root: EMPTY_ROOT,
        ommers_hash: EMPTY_LIST_HASH,
        transactions_root: EMPTY_ROOT,
    };
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

    txn.set(&tables::LastHeader, (Default::default(), block_hash))
        .await?;

    txn.set(&tables::Receipt, (0.into(), vec![])).await?;

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
}
