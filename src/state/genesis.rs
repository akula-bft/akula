use crate::{
    kv::{
        tables::{self, CumulativeData},
        traits::*,
    },
    models::*,
    state::*,
};
use tempfile::TempDir;

#[derive(Clone, Debug)]
pub struct GenesisState {
    chain_spec: ChainSpec,
}

impl GenesisState {
    pub fn new(chain_spec: ChainSpec) -> Self {
        Self { chain_spec }
    }
}

impl GenesisState {
    pub fn initial_state(&self) -> InMemoryState {
        let mut state_buffer = InMemoryState::new();
        // Allocate accounts
        if let Some(balances) = self.chain_spec.balances.get(&BlockNumber(0)) {
            for (&address, &balance) in balances {
                let current_account = Account {
                    balance,
                    ..Default::default()
                };
                state_buffer.update_account(address, None, Some(current_account));
            }
        }
        state_buffer
    }

    pub fn header(&self, initial_state: &InMemoryState) -> BlockHeader {
        let genesis = &self.chain_spec.genesis;
        let seal = &genesis.seal;
        let state_root = initial_state.state_root_hash();

        BlockHeader {
            parent_hash: H256::zero(),
            beneficiary: genesis.author,
            state_root,
            logs_bloom: Bloom::zero(),
            difficulty: seal.difficulty(),
            number: BlockNumber(0),
            gas_limit: genesis.gas_limit,
            gas_used: 0,
            timestamp: genesis.timestamp,
            extra_data: seal.extra_data(),
            mix_hash: seal.mix_hash(),
            nonce: seal.nonce(),
            base_fee_per_gas: None,

            receipts_root: EMPTY_ROOT,
            ommers_hash: EMPTY_LIST_HASH,
            transactions_root: EMPTY_ROOT,
        }
    }
}

pub async fn initialize_genesis<'db, Tx>(
    txn: &Tx,
    etl_temp_dir: &TempDir,
    chainspec: ChainSpec,
) -> anyhow::Result<bool>
where
    Tx: MutableTransaction<'db>,
{
    let genesis = chainspec.genesis.number;
    if txn.get(tables::CanonicalHeader, genesis).await?.is_some() {
        return Ok(false);
    }

    let mut state_buffer = Buffer::new(txn, genesis, None);
    state_buffer.begin_block(genesis);
    // Allocate accounts
    if let Some(balances) = chainspec.balances.get(&genesis) {
        for (&address, &balance) in balances {
            state_buffer.update_account(
                address,
                None,
                Some(Account {
                    balance,
                    ..Default::default()
                }),
            );
        }
    }

    state_buffer.write_to_db().await?;

    crate::stages::promote_clean_accounts(txn, etl_temp_dir).await?;
    crate::stages::promote_clean_storage(txn, etl_temp_dir).await?;
    let state_root = crate::trie::regenerate_intermediate_hashes(txn, etl_temp_dir, None).await?;

    let header = BlockHeader {
        parent_hash: H256::zero(),
        beneficiary: chainspec.genesis.author,
        state_root,
        logs_bloom: Bloom::zero(),
        difficulty: chainspec.genesis.seal.difficulty(),
        number: genesis,
        gas_limit: chainspec.genesis.gas_limit,
        gas_used: 0,
        timestamp: chainspec.genesis.timestamp,
        extra_data: chainspec.genesis.seal.extra_data(),
        mix_hash: chainspec.genesis.seal.mix_hash(),
        nonce: chainspec.genesis.seal.nonce(),
        base_fee_per_gas: None,

        receipts_root: EMPTY_ROOT,
        ommers_hash: EMPTY_LIST_HASH,
        transactions_root: EMPTY_ROOT,
    };
    let block_hash = header.hash();

    txn.set(tables::Header, (genesis, block_hash), header.clone())
        .await?;
    txn.set(tables::CanonicalHeader, genesis, block_hash)
        .await?;
    txn.set(tables::HeaderNumber, block_hash, genesis).await?;
    txn.set(
        tables::HeadersTotalDifficulty,
        (genesis, block_hash),
        header.difficulty,
    )
    .await?;

    txn.set(
        tables::BlockBody,
        (genesis, block_hash),
        BodyForStorage {
            base_tx_id: 0.into(),
            tx_amount: 0,
            uncles: vec![],
        },
    )
    .await?;

    txn.set(
        tables::CumulativeIndex,
        genesis,
        CumulativeData { gas: 0, tx_num: 0 },
    )
    .await?;

    txn.set(tables::LastHeader, Default::default(), block_hash)
        .await?;

    txn.set(tables::Config, block_hash, chainspec).await?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_database;
    use hex_literal::hex;

    fn genesis_header_hash(chain_spec: &'static ChainSpec) -> H256 {
        let genesis = GenesisState::new(chain_spec.clone());
        let genesis_header = genesis.header(&genesis.initial_state());
        genesis_header.hash()
    }

    #[test]
    fn test_genesis_header_hashes() {
        assert_eq!(
            genesis_header_hash(&crate::res::chainspec::MAINNET),
            hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3").into()
        );
        assert_eq!(
            genesis_header_hash(&crate::res::chainspec::ROPSTEN),
            hex!("41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d").into()
        );
        // TODO: fix rinkeby.ron so that the genesis_header_hash is correct
        // assert_eq!(
        //     genesis_header_hash(&crate::res::chainspec::RINKEBY),
        //     hex!("6341fd3daf94b748c72ced5a5b26028f2474f5f00d824504e4fa37a75767e177").into()
        // );
    }

    #[tokio::test]
    async fn init_mainnet_genesis() {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        let temp_dir = TempDir::new().unwrap();
        assert!(
            initialize_genesis(&tx, &temp_dir, crate::res::chainspec::MAINNET.clone())
                .await
                .unwrap()
        );

        let genesis_hash = tx
            .get(tables::CanonicalHeader, 0.into())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            genesis_hash,
            hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3").into()
        );
    }
}
