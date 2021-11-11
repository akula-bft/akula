use super::genesis::GenesisState;
use crate::{
    models::{Account, BlockHeader, BlockNumber, ChainSpec, Seal, EMPTY_LIST_HASH, EMPTY_ROOT},
    InMemoryState,
};
use ethereum_types::{Bloom, H256, H64};

#[derive(Clone, Debug)]
pub struct GenesisChainSpec {
    chain_spec: &'static ChainSpec,
}

impl GenesisChainSpec {
    pub fn new(chain_spec: &'static ChainSpec) -> Self {
        Self { chain_spec }
    }
}

impl GenesisState for GenesisChainSpec {
    fn initial_state(&self) -> InMemoryState {
        let mut state_buffer = InMemoryState::new();
        // Allocate accounts
        for (address, balance) in &self.chain_spec.balances[&BlockNumber(0)] {
            let current_account = Account {
                balance: *balance,
                ..Default::default()
            };
            state_buffer.update_account_sync(*address, None, Some(current_account));
        }
        state_buffer
    }

    fn header(&self, initial_state: &InMemoryState) -> BlockHeader {
        let genesis = &self.chain_spec.genesis;
        let state_root = initial_state.state_root_hash();

        let mix_hash = match self.chain_spec.genesis.seal {
            Seal::Ethash { mix_hash, .. } => mix_hash,
            _ => H256::zero(),
        };

        let nonce = match self.chain_spec.genesis.seal {
            Seal::Ethash { nonce, .. } => nonce,
            _ => H64::zero(),
        };

        BlockHeader {
            parent_hash: H256::zero(),
            beneficiary: genesis.author,
            state_root,
            logs_bloom: Bloom::zero(),
            difficulty: genesis.difficulty,
            number: BlockNumber(0),
            gas_limit: genesis.gas_limit,
            gas_used: 0,
            timestamp: genesis.timestamp,
            extra_data: genesis.extra_data.clone(),
            mix_hash,
            nonce,
            base_fee_per_gas: None,

            receipts_root: EMPTY_ROOT,
            ommers_hash: EMPTY_LIST_HASH,
            transactions_root: EMPTY_ROOT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ChainSpec;
    use ethereum_types::H256;
    use hex_literal::hex;

    fn genesis_header_hash(chain_spec: &'static ChainSpec) -> H256 {
        let genesis = GenesisChainSpec::new(chain_spec);
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
}
