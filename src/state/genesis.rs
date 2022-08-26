use crate::{
    kv::{mdbx::*, tables},
    models::*,
    res::chainspec::MAINNET,
    state::*,
};
use anyhow::format_err;
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
        let block_number = BlockNumber(0);
        if let Some(balances) = self.chain_spec.balances.get(&block_number) {
            for (&address, &balance) in balances {
                let code_hash = self.chain_spec.try_get_code_with_hash(block_number, &address)
                    .map(|(hash, _)| hash).unwrap_or(EMPTY_HASH);
                println!("initial_state address{:?}, code_hash:{:?}", address, code_hash);
                let current_account = Account {
                    balance,
                    code_hash,
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
            base_fee_per_gas: genesis.base_fee_per_gas,

            receipts_root: EMPTY_ROOT,
            ommers_hash: EMPTY_LIST_HASH,
            transactions_root: EMPTY_ROOT,
        }
    }
}

pub fn initialize_genesis<'db, E>(
    txn: &MdbxTransaction<'db, RW, E>,
    etl_temp_dir: &TempDir,
    bundled_chain_spec: bool,
    chainspec: Option<ChainSpec>,
) -> anyhow::Result<(ChainSpec, bool)>
where
    E: EnvironmentKind,
{
    if let Some(existing_chainspec) = txn.get(tables::Config, ())? {
        if let Some(chainspec) = chainspec {
            if chainspec != existing_chainspec {
                println!("initialize_genesis chainspec.name: {:?}: {:?}", chainspec.name, existing_chainspec.name);
                if bundled_chain_spec && chainspec.name == existing_chainspec.name {
                    txn.set(tables::Config, (), chainspec.clone())?;
                    return Ok((chainspec, true));
                } else {
                    return Err(format_err!(
                        "Genesis initialized, but chainspec does not match one in database"
                    ));
                }
            }
        }
        return Ok((existing_chainspec, false));
    }

    let chainspec = chainspec.unwrap_or_else(|| MAINNET.clone());

    let genesis = chainspec.genesis.number;
    let mut state_buffer = Buffer::new(txn, None);
    state_buffer.begin_block(genesis);
    // Allocate accounts
    if let Some(balances) = chainspec.balances.get(&genesis) {
        for (&address, &balance) in balances {
            let code_hash = if let Some((hash, code)) = chainspec.try_get_code_with_hash(genesis, &address) {
                state_buffer.update_code(hash, code)?;
                hash
            } else {
                EMPTY_HASH
            };
            println!("initialize_genesis address{:?}, code_hash:{:?}", address, code_hash);
            state_buffer.update_account(
                address,
                None,
                Some(Account {
                    balance,
                    code_hash,
                    ..Default::default()
                }),
            );
        }
    }

    state_buffer.write_to_db()?;

    crate::stages::promote_clean_accounts(txn, etl_temp_dir)?;
    crate::stages::promote_clean_storage(txn, etl_temp_dir)?;
    let state_root = crate::trie::regenerate_intermediate_hashes(txn, etl_temp_dir, None)?;

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
        base_fee_per_gas: chainspec.genesis.base_fee_per_gas,

        receipts_root: EMPTY_ROOT,
        ommers_hash: EMPTY_LIST_HASH,
        transactions_root: EMPTY_ROOT,
    };
    let block_hash = header.hash();
    println!("initialize_genesis block_hash:{:?}", block_hash);


    txn.set(tables::Header, (genesis, block_hash), header.clone())?;
    txn.set(tables::CanonicalHeader, genesis, block_hash)?;
    txn.set(tables::HeaderNumber, block_hash, genesis)?;
    txn.set(
        tables::HeadersTotalDifficulty,
        (genesis, block_hash),
        header.difficulty,
    )?;

    txn.set(
        tables::BlockBody,
        (genesis, block_hash),
        BodyForStorage {
            base_tx_id: 0.into(),
            tx_amount: 0,
            uncles: vec![],
        },
    )?;

    txn.set(tables::TotalGas, genesis, 0)?;
    txn.set(tables::TotalTx, genesis, 0)?;

    txn.set(tables::LastHeader, (), (BlockNumber(0), block_hash))?;

    txn.set(tables::Config, (), chainspec.clone())?;

    Ok((chainspec, true))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv::new_mem_chaindata;
    use hex_literal::hex;

    #[test]
    fn test_genesis_hashes() {
        for (chainspec, hash) in [
            (
                &crate::res::chainspec::MAINNET,
                hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3"),
            ),
            (
                &crate::res::chainspec::ROPSTEN,
                hex!("41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d"),
            ),
            (
                &crate::res::chainspec::RINKEBY,
                hex!("6341fd3daf94b748c72ced5a5b26028f2474f5f00d824504e4fa37a75767e177"),
            ),
            (
                &crate::res::chainspec::SEPOLIA,
                hex!("25a5cc106eea7138acab33231d7160d69cb777ee0c2c553fcddf5138993e6dd9"),
            ),
        ] {
            let hash = H256(hash);

            {
                let genesis = GenesisState::new((*chainspec).clone());
                let genesis_header = genesis.header(&genesis.initial_state());
                assert_eq!(genesis_header.hash(), hash);
            }

            let db = new_mem_chaindata().unwrap();
            let tx = db.begin_mutable().unwrap();

            let temp_dir = TempDir::new().unwrap();
            assert!(
                initialize_genesis(&tx, &temp_dir, false, Some((*chainspec).clone()))
                    .unwrap()
                    .1
            );

            assert_eq!(
                tx.get(tables::CanonicalHeader, 0.into()).unwrap().unwrap(),
                hash
            );
        }
    }
}
